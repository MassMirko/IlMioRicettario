#!/usr/bin/env python3
"""Incremental, non-destructive sync from Windows ZIP to Cloudflare D1 catalog.

No deletion, no resets of votes, authors or identities. Defaults to dry-run.
Runs inside GitHub Actions. Only the workflow determines the allowed D1 database.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
import uuid
import zipfile

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = ROOT / 'sync' / 'ricettario_dati.zip'
MANIFEST = ROOT / 'sync' / 'manifest.json'
REPO = 'MassMirko/IlMioRicettario'
TEST_DB = 'f1fc18be-82ea-446a-911d-06b9a9f78b2e'
PROD_DB = 'b22d8825-9259-4cce-92f5-8124fe6f8c7a'
COLS = ('nome', 'autore', 'tipo_piatto', 'ingrediente_principale', 'regione',
        'ingredienti', 'preparazione', 'tempo_preparazione', 'tempo_cottura',
        'persone', 'difficolta', 'note', 'video_link')
ORIGINAL_COLS = ('id', 'nome', 'tipo_piatto', 'ingrediente_principale', 'regione',
                 'foto', 'ingredienti', 'preparazione', 'tempo_preparazione',
                 'tempo_cottura', 'persone', 'difficolta', 'note', 'video_link',
                 'preferita', 'autore')
META_PHOTO = '_sync_foto_sha256'


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def request_json(url: str, method: str, token: str, body: dict | None = None, *, allow_404=False):
    data = json.dumps(body, ensure_ascii=False).encode('utf-8') if body is not None else None
    req = Request(url, data=data, method=method, headers={
        'Authorization': f'Bearer {token}', 'Accept': 'application/vnd.github+json' if 'api.github.com' in url else 'application/json',
        'Content-Type': 'application/json', 'User-Agent': 'IlMioRicettario-Sync/1.0'
    })
    try:
        with urlopen(req, timeout=45) as resp:
            return json.load(resp)
    except HTTPError as exc:
        if allow_404 and exc.code == 404:
            return None
        # Do not print headers, complete URLs with sensitive query values or auth credentials.
        raise RuntimeError(f'HTTP {exc.code} calling {method} {url.split("?")[0]}') from None


class D1:
    def __init__(self):
        account = os.environ.get('CF_ACCOUNT', '').strip()
        database = os.environ.get('CF_DATABASE_ID', '').strip()
        self.token = os.environ.get('CF_TOKEN', '').strip()
        require(len(account) == 32 and all(c in '0123456789abcdef' for c in account), 'Account ID Cloudflare mancante o errato')
        require(database in (TEST_DB, PROD_DB), 'Database D1 non autorizzato')
        self.production = database == PROD_DB
        require(bool(self.token), 'Token Cloudflare mancante')
        self.url = f'https://api.cloudflare.com/client/v4/accounts/{account}/d1/database/{database}/query'

    def query(self, sql: str, params: list[str] | None = None):
        result = request_json(self.url, 'POST', self.token, {'sql': sql, 'params': params or []})
        require(result.get('success') is True and result.get('result'), 'Query D1 fallita')
        require(all(x.get('success') is True for x in result['result']), 'Una o piu query D1 non riuscite')
        return result['result'][0]['results']

    def batch(self, statements: list[tuple[str, list[str]]]):
        if not statements:
            return
        payload = {'batch': [{'sql': s, 'params': p} for s, p in statements]}
        result = request_json(self.url, 'POST', self.token, payload)
        require(result.get('success') is True and result.get('result'), 'Batch D1 fallito')
        require(len(result['result']) == len(statements) and all(x.get('success') is True for x in result['result']), 'Batch D1 incompleto o fallito')


def source_rows():
    require(ARCHIVE.is_file() and MANIFEST.is_file(), 'Pacchetto sync/ Windows assente')
    manifest = json.loads(MANIFEST.read_text(encoding='utf-8-sig'))
    with zipfile.ZipFile(ARCHIVE) as z:
        require('aggiornamento.db' in z.namelist(), 'Database mancante nello ZIP')
        db_bytes = z.read('aggiornamento.db')
        require(hashlib.sha256(db_bytes).hexdigest() == manifest.get('database_sha256'), 'SHA256 database Windows diverso dal manifest')
        db = sqlite3.connect(':memory:')
        try:
            db.deserialize(db_bytes)
            db.row_factory = sqlite3.Row
            rows = [dict(row) for row in db.execute('SELECT ' + ', '.join(ORIGINAL_COLS) + ' FROM ricette ORDER BY id')]
        finally:
            db.close()
        require(len(rows) >= 65 and len(rows) == manifest.get('recipe_count'), 'Numero ricette ZIP/manifest incoerente (minimo storico: 65)')
        require(len({str(r['id']) for r in rows}) == len(rows), 'ID Windows duplicati')
        require(all(str(r['nome'] or '').strip() for r in rows), 'Ricetta senza nome')
        require(manifest.get('image_count') == len(rows), 'Numero fotografie nel manifest incoerente')
        zipnames = set(z.namelist())
        images = {}
        for row in rows:
            rawpath = str(row['foto'] or '').replace('\\', '/').lstrip('./')
            require(rawpath.startswith('immagini/') and '..' not in rawpath.split('/') and rawpath in zipnames,
                    f'Foto originale mancante nello ZIP per ID {row["id"]}: {rawpath}')
            data = z.read(rawpath)
            require(bool(data), f'Foto vuota per ID {row["id"]}')
            suffix = Path(rawpath).suffix.lower()
            require(suffix in ('.png', '.jpg', '.jpeg', '.webp'), f'Formato fotografia inatteso ID {row["id"]}: {suffix}')
            digest = hashlib.sha256(data).hexdigest()
            images[str(row['id'])] = (data, digest, suffix, rawpath)
        return rows, images


def git_blob_sha(data: bytes) -> str:
    return hashlib.sha1(b'blob ' + str(len(data)).encode('ascii') + b'\0' + data).hexdigest()


def upload_photo(image: tuple, token: str) -> str:
    data, digest, suffix, _original = image
    path = f'immagini/sync-{digest}{suffix}'
    endpoint = f'https://api.github.com/repos/{REPO}/contents/{quote(path)}'
    expected_blob = git_blob_sha(data)
    present = request_json(endpoint + '?ref=main', 'GET', token, allow_404=True)
    if present is None:
        created = request_json(endpoint, 'PUT', token, {
            'message': f'Sync: fotografia originale {digest[:12]}',
            'branch': 'main', 'content': base64.b64encode(data).decode('ascii')
        })
        require(created.get('content', {}).get('sha') == expected_blob, f'SHA fotografia non verificato dopo upload: {path}')
        print(f'Foto pubblicata: {path}')
    else:
        require(present.get('sha') == expected_blob, f'Foto GitHub con contenuto inatteso: {path}')
        print(f'Foto gia presente: {path}')
    return f'https://raw.githubusercontent.com/{REPO}/main/{path}'


def sql_value(v) -> str:
    return '' if v is None else str(v)


def main():
    apply = os.environ.get('SYNC_APPLY', 'false').strip().lower() == 'true'
    db = D1()
    if db.production:
        require(os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch',
                'BLOCCO: PRODUZIONE consentita soltanto con avvio manuale')
        if apply:
            require(os.environ.get('SYNC_PRODUCTION_CONFIRM') == 'CONFERMO_PRODUZIONE',
                    'BLOCCO: manca la conferma esplicita per PRODUZIONE')
    print('TARGET:', 'PRODUZIONE' if db.production else 'TEST',
          'MODALITA:', 'APPLICA' if apply else 'ANTEPRIMA (sola lettura)')
    rows, images = source_rows()
    existing_refs = {str(x['id_originale']): x for x in db.query(
        "SELECT id_originale, ricetta_chiave, dati_originali_json FROM riferimenti_catalogo_unificato WHERE origine='sync'")}
    catalog = {x['chiave']: x for x in db.query(
        "SELECT chiave, nome, stato, foto_url FROM catalogo_ricette_unificato")}
    names = {}
    for entry in catalog.values():
        names.setdefault(str(entry['nome']).strip().casefold(), []).append(entry['chiave'])

    proposals = []
    for row in rows:
        rid = str(row['id'])
        ref = existing_refs.get(rid)
        img = images[rid]
        fresh_data = {k: row.get(k) for k in ORIGINAL_COLS}
        fresh_digest = img[1]
        if ref:
            key = ref['ricetta_chiave']
            require(key in catalog, f'Riferimento sync {rid} orfano: {key}')
            require(catalog[key]['stato'] == 'approvata', f'Ricetta sync {rid} non approvata: STOP')
            try:
                prior = json.loads(ref['dati_originali_json'])
                require(isinstance(prior, dict), f'JSON sync {rid} non valido')
            except (ValueError, TypeError):
                raise RuntimeError(f'JSON sync {rid} non valido') from None
            old_digest = prior.get(META_PHOTO)
            source_changed = any(sql_value(prior.get(k)) != sql_value(row.get(k)) for k in COLS + ('foto',))
            image_changed = bool(old_digest and old_digest != fresh_digest)
            # First run establishes a hash baseline; unchanged legacy photos and public URLs are retained.
            needs_image = image_changed or (source_changed and not old_digest and
                            sql_value(prior.get('foto')) != sql_value(row.get('foto')))
            if source_changed or image_changed or not old_digest:
                proposals.append(('update' if (source_changed or image_changed) else 'baseline', rid, key, row, prior, needs_image))
        else:
            title = str(row['nome']).strip().casefold()
            matches = names.get(title, [])
            if matches:
                require(db.production and rid == '69'
                        and title == 'tagliata di manzo alla piastra con cavolo cappuccio bianco in insalata'
                        and matches == ['f050d6db-63a1-41ed-adcf-ad1f14775daa']
                        and catalog[matches[0]]['stato'] == 'approvata',
                        f'Corrispondenza ricetta non autorizzata: {row["nome"]}')
                linked = db.query(
                    "SELECT ricetta_chiave FROM riferimenti_catalogo_unificato "
                    "WHERE origine='cloudflare' AND id_originale='12'")
                require(len(linked) == 1 and linked[0]['ricetta_chiave'] == matches[0],
                        'Riferimento cloudflare/12 inatteso: STOP')
                proposals.append(('link', rid, matches[0], row, None, False))
                continue
            key = str(uuid.uuid5(uuid.NAMESPACE_URL, f'https://github.com/{REPO}/sync/{rid}'))
            require(key not in catalog, f'Collisione UUID per ricetta Windows ID {rid}')
            proposals.append(('new', rid, key, row, None, True))
            names[title] = [key]

    n_new = sum(p[0] == 'new' for p in proposals)
    n_update = sum(p[0] == 'update' for p in proposals)
    n_baseline = sum(p[0] == 'baseline' for p in proposals)
    n_photos = sum(p[-1] for p in proposals)
    print(f'Originali Windows: {len(rows)}; riferimenti sync gia presenti: {len(existing_refs)}')
    print(f'Nuove ricette: {n_new}; ricette modificate: {n_update}; riferimento hash iniziale: {n_baseline}; foto da pubblicare: {n_photos}')
    for kind, rid, key, row, old, needs_img in proposals:
        if kind != 'baseline':
            print(f'{kind.upper()} ID {rid}: {row["nome"]!r} | foto={"SI" if needs_img else "NO"}')
    if not apply:
        print('ANTEPRIMA COMPLETATA: zero scritture GitHub/Cloudflare')
        return

    gh_token = os.environ.get('GH_TOKEN', '').strip()
    require(bool(gh_token), 'Token GITHUB_TOKEN mancante')
    for kind, rid, key, row, old, needs_img in proposals:
        current = catalog.get(key)
        photo_url = upload_photo(images[rid], gh_token) if needs_img else (current['foto_url'] if current else '')
        require(bool(photo_url), f'URL foto assente ID {rid}')
        payload = dict(old or {})
        payload.update({k: row.get(k) for k in ORIGINAL_COLS})
        payload[META_PHOTO] = images[rid][1]
        data_json = json.dumps(payload, ensure_ascii=False, separators=(',', ':'))
        if kind == 'link':
            statements = [(
                "INSERT INTO riferimenti_catalogo_unificato "
                "(origine, id_originale, ricetta_chiave, dati_originali_json) "
                "VALUES ('sync', ?, ?, ?) "
                "ON CONFLICT(origine, id_originale) DO NOTHING",
                [rid, key, data_json])]
        elif kind == 'new':
            fields = ('chiave',) + COLS + ('foto_url', 'foto_riferimento', 'stato')
            vals = [key] + [sql_value(row.get(c)) for c in COLS] + [photo_url, images[rid][3], 'approvata']
            sql = (f'INSERT INTO catalogo_ricette_unificato ({", ".join(fields)}) '
                   f'VALUES ({", ".join("?" for _ in fields)}) ON CONFLICT(chiave) DO NOTHING')
            statements = [(sql, vals), (
                "INSERT INTO riferimenti_catalogo_unificato (origine, id_originale, ricetta_chiave, dati_originali_json) VALUES ('sync', ?, ?, ?) ON CONFLICT(origine, id_originale) DO NOTHING",
                [rid, key, data_json])]
        else:
            old_hash = (old or {}).get(META_PHOTO)
            source_changed = any(sql_value((old or {}).get(k)) != sql_value(row.get(k)) for k in COLS + ('foto',))
            statements = []
            if source_changed or (old_hash and old_hash != images[rid][1]):
                fields = COLS + ('foto_url', 'foto_riferimento', 'data_modifica')
                updates = ', '.join(f'{c}=?' for c in fields[:-1]) + ', data_modifica=CURRENT_TIMESTAMP'
                vals = [sql_value(row.get(c)) for c in COLS] + [photo_url, images[rid][3], key]
                statements.append((f"UPDATE catalogo_ricette_unificato SET {updates} WHERE chiave=? AND stato='approvata'", vals))
            statements.append(("UPDATE riferimenti_catalogo_unificato SET dati_originali_json=? WHERE origine='sync' AND id_originale=? AND ricetta_chiave=?", [data_json, rid, key]))
        db.batch(statements)
        verify = db.query("SELECT r.ricetta_chiave AS chiave,c.foto_url AS foto FROM riferimenti_catalogo_unificato r JOIN catalogo_ricette_unificato c ON c.chiave=r.ricetta_chiave WHERE r.origine='sync' AND r.id_originale=?", [rid])
        require(len(verify) == 1 and verify[0]['chiave'] == key and verify[0]['foto'] == photo_url, f'Verifica scrittura ID {rid} fallita')
        if kind != 'baseline':
            print(f'OK ID {rid}: UUID mantenuto/registrato: {key}')
    final_count = db.query("SELECT COUNT(*) AS totale FROM riferimenti_catalogo_unificato WHERE origine='sync'")[0]['totale']
    require(final_count == len(rows), f'Riferimenti sync attesi {len(rows)}, trovati {final_count}')
    print(f'COMPLETATO: {final_count} riferimenti sync. Nessuna cancellazione; voti e proposte preservati.')


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, sqlite3.Error, zipfile.BadZipFile, OSError, ValueError) as exc:
        print(f'ERRORE: {exc}', file=sys.stderr)
        sys.exit(1)
