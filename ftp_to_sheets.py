#!/usr/bin/env python3
# ftp_to_sheets.py - robusted for GitHub Actions troubleshooting
import os
import io
import sys
import json
import traceback
from ftplib import FTP
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def log(msg):
    print(msg, flush=True)

def required_env(name):
    v = os.environ.get(name)
    if not v:
        log(f"ERROR: required env var {name} is not set.")
        sys.exit(1)
    return v

def get_mdtm(ftp, name):
    try:
        resp = ftp.sendcmd("MDTM " + name)  # e.g. "213 20250814010001"
        if resp.startswith("213"):
            ts = resp.split()[1].strip()
            return datetime.strptime(ts, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def load_df_from_bytes(filename, data_bytes):
    bio = io.BytesIO(data_bytes)
    ext = os.path.splitext(filename)[1].lower()
    try:
        if ext in [".csv", ".txt"]:
            bio.seek(0)
            return pd.read_csv(bio)
        if ext in [".xls", ".xlsx"]:
            bio.seek(0)
            return pd.read_excel(bio)
        bio.seek(0)
        return pd.read_csv(bio)
    except Exception as e:
        try:
            bio.seek(0)
            return pd.read_csv(bio, encoding="latin1")
        except Exception:
            log(f"Failed to parse {filename}: {e}")
            raise

# ----------------- Config via env (GitHub Secrets) -----------------
FTP_HOST = required_env("FTP_HOST")
FTP_USER = required_env("FTP_USER")
FTP_PASS = required_env("FTP_PASS")
FTP_DIR  = required_env("FTP_DIR")

SPREADSHEET_ID = required_env("SPREADSHEET_ID")
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "OOCL_import")

GOOGLE_SERVICE_ACCOUNT_JSON = required_env("GOOGLE_SERVICE_ACCOUNT_JSON")

A1Z_RANGE = "A1:Z"
START_CELL = "A1"
MAX_COLS   = 26  # A..Z

# ----------------- Google Sheets auth -----------------
try:
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
except Exception as e:
    log("ERROR: GOOGLE_SERVICE_ACCOUNT_JSON could not be parsed as JSON.")
    log(str(e))
    sys.exit(1)

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

def main():
    try:
        log("Connecting to FTP...")
        ftp = FTP(FTP_HOST, timeout=60)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_DIR)

        try:
            names = ftp.nlst()
        except Exception:
            lines = []
            ftp.retrlines("LIST", lines.append)
            names = [ln.split()[-1] for ln in lines if ln.strip()]

        candidates = []
        for n in names:
            if n in (".", ".."):
                continue
            ext = os.path.splitext(n)[1].lower()
            if ext not in [".csv", ".txt", ".xls", ".xlsx"]:
                continue
            try:
                size = ftp.size(n)
            except Exception:
                continue
            mdtm = get_mdtm(ftp, n)
            candidates.append((n, mdtm, size))

        if not candidates:
            log("No candidate files found in FTP folder.")
            ftp.quit()
            return

        if any(c[1] for c in candidates):
            newest = max(candidates, key=lambda t: (t[1] or datetime.min.replace(tzinfo=timezone.utc), t[0]))
        else:
            newest = sorted(candidates, key=lambda t: t[0])[-1]

        filename = newest[0]
        log(f"Latest file: {filename} (size={newest[2]} mtime={newest[1]})")

        bio = io.BytesIO()
        ftp.retrbinary(f"RETR {filename}", bio.write)
        ftp.quit()
        data_bytes = bio.getvalue()

        df = load_df_from_bytes(filename, data_bytes)
        log(f"Loaded dataframe: {df.shape[0]} rows × {df.shape[1]} cols")

        if df.shape[1] > MAX_COLS:
            log(f"Truncating columns from {df.shape[1]} to first {MAX_COLS} (A..Z).")
            df = df.iloc[:, :MAX_COLS]

        df = df.replace([np.inf, -np.inf], None).fillna("")

        headers = [str(c) for c in df.columns.tolist()]
        values = [headers] + df.astype(object).values.tolist()

        sh = client.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet(WORKSHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=WORKSHEET_NAME, rows=str(max(100, len(values)+10)), cols=str(MAX_COLS))

        # Step 1: Clear A1:Z ONLY
        log("Step 1: Clearing A1:Z only...")
        try:
            ws.batch_clear([A1Z_RANGE])
        except Exception:
            ws.spreadsheet.values_clear(f"'{ws.title}'!{A1Z_RANGE}")

        # Ensure enough rows for paste
        needed_rows = len(values)
        if ws.row_count < needed_rows:
            ws.add_rows(needed_rows - ws.row_count)

        # Step 2: Paste data to A1:Z
        log("Step 2: Pasting data...")
        trimmed = [row[:MAX_COLS] for row in values]
        if trimmed:
            ws.update(START_CELL, trimmed)

        # Step 3: Insert new blank row at top (safely provide correct length)
        log("Step 3: Inserting blank row at top...")
        ws.insert_row([''] * MAX_COLS, 1)

        # Step 4: Write timestamp at A1
        vn_now = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
        timestamp = vn_now.strftime("Report synced at %I:%M %p | %d-%b-%Y")
        ws.update("A1", [[timestamp]])

        # Step 5: Write source filename at D1
        ws.update("D1", [[f"Source file name: {filename}"]])

        log("✅ Done. Data now at A2:Z; timestamp in A1; source filename in D1.")
    except Exception as exc:
        log("ERROR during run:")
        log(traceback.format_exc())
        # Raise non-zero exit so Actions shows failed run (helps debugging)
        raise

if __name__ == "__main__":
    main()
