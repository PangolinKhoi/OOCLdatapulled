import os
import io
from datetime import datetime, timezone
from ftplib import FTP, error_perm
import pandas as pd
import json

from google.oauth2 import service_account
from googleapiclient.discovery import build


def connect_ftp(host: str, user: str, pwd: str) -> FTP:
    ftp = FTP(host, timeout=60)
    ftp.login(user=user, passwd=pwd)
    return ftp


def list_files_with_mtime(ftp: FTP, directory: str):
    """
    Returns list of tuples: (name, mtime_datetime_utc, size)
    Tries MDTM for reliable modification time; falls back to LIST parsing if needed.
    """
    ftp.cwd(directory)

    # Prefer NLST for names first
    try:
        names = ftp.nlst()
    except error_perm as e:
        # If NLST blocked, fallback to LIST parse (last token is name, less reliable)
        lines = []
        ftp.retrlines('LIST', lines.append)
        names = [ln.split()[-1] for ln in lines if ln.strip()]
    
    files = []
    for name in names:
        # Skip . and .., hidden dirs, etc.
        if name in ('.', '..'):
            continue
        # Try getting size; if it fails, it might be a directory—skip it
        try:
            size = ftp.size(name)
        except Exception:
            continue  # likely a directory
        
        # Try MDTM for accurate timestamp
        mtime = None
        try:
            resp = ftp.sendcmd(f"MDTM {name}")  # e.g. "213 20240730113045"
            if resp.startswith("213"):
                ts = resp.split()[1].strip()
                mtime = datetime.strptime(ts, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        except Exception:
            pass

        # If MDTM not allowed, last resort: assume recent by name order (not ideal)
        if mtime is None:
            # We could try MLSD, but many servers restrict it; keep None
            mtime = datetime.min.replace(tzinfo=timezone.utc)
        
        files.append((name, mtime, size))
    return files


def get_newest_file(ftp: FTP, directory: str):
    files = list_files_with_mtime(ftp, directory)
    if not files:
        raise RuntimeError("No files found in FTP directory.")
    # Prefer max by mtime; if mtime missing, datetime.min will push those to the bottom
    newest = max(files, key=lambda t: (t[1], t[0]))
    return newest  # (name, mtime, size)


def download_file(ftp: FTP, filename: str) -> bytes:
    bio = io.BytesIO()
    ftp.retrbinary(f"RETR {filename}", bio.write)
    return bio.getvalue()


def load_dataframe_from_bytes(filename: str, data: bytes) -> pd.DataFrame:
    name = filename.lower()
    bio = io.BytesIO(data)
    # Try based on extension first
    if name.endswith(".csv"):
        return pd.read_csv(bio)
    if name.endswith(".txt"):
        # common TXT is CSV-like
        try:
            bio.seek(0)
            return pd.read_csv(bio)
        except Exception:
            bio.seek(0)
            return pd.read_table(bio)
    if name.endswith(".xlsx") or name.endswith(".xls"):
        bio.seek(0)
        return pd.read_excel(bio)
    # Fallback: attempt CSV
    bio.seek(0)
    return pd.read_csv(bio)


def update_google_sheet(df: pd.DataFrame, spreadsheet_id: str, sheet_name: str, svc_json_str: str):
    info = json.loads(svc_json_str)
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    service = build("sheets", "v4", credentials=creds)

    # Convert DataFrame to values (header + rows)
    values = [list(map(str, df.columns.tolist()))] + df.astype(object).where(pd.notna(df), "").values.tolist()

    # Clear then update (keeps your formatting rules simple)
    service.spreadsheets().values().clear(
        spreadsheetId=spreadshet_id := spreadsheet_id,
        range=sheet_name
    ).execute()

    service.spreadsheets().values().update(
        spreadsheetId=spreadshet_id,
        range=sheet_name,
        valueInputOption="RAW",
        body={"values": values},
    ).execute()


def main():
    # Env vars (injected by GitHub Actions)
    ftp_host = os.environ["FTP_HOST"]
    ftp_user = os.environ["FTP_USER"]
    ftp_pass = os.environ["FTP_PASS"]
    ftp_dir  = os.environ["FTP_DIR"]

    spreadsheet_id = os.environ["SPREADSHEET_ID"]
    sheet_name = os.environ["SHEET_NAME"]
    svc_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

    ftp = connect_ftp(ftp_host, ftp_user, ftp_pass)
    try:
        newest_name, newest_mtime, newest_size = get_newest_file(ftp, ftp_dir)
        print(f"Newest file: {newest_name} | Size: {newest_size} | mtime: {newest_mtime}")
        ftp.cwd(ftp_dir)
        content = download_file(ftp, newest_name)
    finally:
        try:
            ftp.quit()
        except Exception:
            pass

    df = load_dataframe_from_bytes(newest_name, content)
    print(f"Loaded dataframe: {df.shape[0]} rows x {df.shape[1]} cols")

    update_google_sheet(df, spreadsheet_id, sheet_name, svc_json)
    print("Sheet updated successfully.")


if __name__ == "__main__":
    main()
