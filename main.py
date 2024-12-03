import pandas as pd
from sqlalchemy import Column, Integer, String, Text, BigInteger, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
import pymysql
import re
import pdfplumber
import traceback

Base = declarative_base()

# Model Database untuk Financial Statement
class FinancialStatement(Base):
    __tablename__ = 'financial_statement'
    id = Column(Integer, primary_key=True, autoincrement=True)
    emitent = Column(String(255), nullable=False)
    grup_lk = Column(String(255), nullable=False)
    item = Column(Text, nullable=False)
    value = Column(BigInteger, nullable=False)
    quarter = Column(String(10), nullable=False)
    notes = Column(Text, nullable=True)

# Fungsi untuk mengubah string menjadi float jika memungkinkan
def convert_to_float(value):
    if isinstance(value, str):
        value = value.replace(',', '')
        if re.match(r'^\(.*\)$', value):  # Menangani nilai dalam tanda kurung seperti "(123)"
            value = value.strip('()')
            value = f"-{value}"  # Negasikan nilai
    try:
        return float(value)
    except ValueError:
        return None

# Fungsi untuk ekstraksi nomor catatan dari PDF menggunakan pdfplumber
def extract_notes_from_pdf(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        text = ""
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text

    # Debug: Pastikan seluruh teks diekstraksi dengan benar
    print(f"Extracted Text from PDF (First 1000 chars): {text[:1000]}")
    
    # Regex untuk menemukan catatan yang relevan
    notes = re.findall(r'(?:Catatan|Note)\s*(\d+)', text, re.IGNORECASE)

    # Debug: Pastikan notes yang diekstraksi dari PDF
    print(f"Extracted Notes: {notes}")

    return notes

# Fungsi untuk mengambil nama entitas dari sheet "1000000"
def extract_emitent_name(excel_path):
    xls = pd.ExcelFile(excel_path)
    print(f"Available sheets: {xls.sheet_names}")  # Debug: Cek sheet yang tersedia
    
    entitas_df = pd.read_excel(xls, sheet_name="1000000", header=None)
    print(f"First rows of '1000000':\n{entitas_df.head()}")  # Debug: Cek struktur data

    # Menemukan baris yang berisi "Nama entitas" dan mengambil nilai pada kolom berikutnya
    emitent_name = entitas_df[entitas_df.iloc[:, 0] == "Nama entitas"].iloc[0, 1]
    print(f"Emitent Name: {emitent_name}")  # Debug: Cek nama entitas
    return emitent_name

# Fungsi untuk mengekstrak dan memasukkan data Laba Rugi
def extract_and_insert_laba_rugi(excel_path, pdf_path, session, emitent_name):
    xls = pd.ExcelFile(excel_path)
    laba_rugi_df = pd.read_excel(xls, sheet_name="1311000", header=1)
    print(f"Laba Rugi Data (First rows):\n{laba_rugi_df.head()}")  # Debug: Cek struktur data

    laba_rugi_df = laba_rugi_df[['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3']].dropna()
    laba_rugi_df.columns = ['category', 'current_year', 'prior_year', 'statement_profit_or_loss']
    laba_rugi_df['current_year'] = laba_rugi_df['current_year'].apply(convert_to_float)
    laba_rugi_df['prior_year'] = laba_rugi_df['prior_year'].apply(convert_to_float)

    # Ambil catatan dari PDF
    laba_rugi_notes = extract_notes_from_pdf(pdf_path)

    for idx, row in laba_rugi_df.iterrows():
        note_number = laba_rugi_notes[idx] if idx < len(laba_rugi_notes) else None
        print(f"Inserting: Item: {row['category']}, Note: {note_number}")  # Debug

        session.add(FinancialStatement(
            emitent=emitent_name,
            grup_lk='Laba Rugi',
            item=row['category'] if row['category'] else "N/A",
            value=int(row['current_year']) if row['current_year'] else 0,
            quarter="Q1",
            notes=note_number
        ))

# Fungsi untuk mengekstrak dan memasukkan data Arus Kas
def extract_and_insert_arus_kas(excel_path, pdf_path, session, emitent_name):
    xls = pd.ExcelFile(excel_path)
    arus_kas_df = pd.read_excel(xls, sheet_name="1510000", header=1)
    print(f"Arus Kas Data (First rows):\n{arus_kas_df.head()}")  # Debug

    arus_kas_df = arus_kas_df[['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3']].dropna()
    arus_kas_df.columns = ['category', 'current_year_instant', 'prior_year_duration', 'statement_cash_flow']
    arus_kas_df['current_year_instant'] = arus_kas_df['current_year_instant'].apply(convert_to_float)
    arus_kas_df['prior_year_duration'] = arus_kas_df['prior_year_duration'].apply(convert_to_float)

    arus_kas_notes = extract_notes_from_pdf(pdf_path)

    for idx, row in arus_kas_df.iterrows():
        note_number = arus_kas_notes[idx] if idx < len(arus_kas_notes) else None
        print(f"Inserting: Item: {row['category']}, Note: {note_number}")  # Debug

        session.add(FinancialStatement(
            emitent=emitent_name,
            grup_lk='Arus Kas',
            item=row['category'] if row['category'] else "N/A",
            value=int(row['current_year_instant']) if row['current_year_instant'] else 0,
            quarter="Q1",
            notes=note_number
        ))

# Fungsi untuk mengekstrak dan memasukkan data Laporan Posisi Keuangan
def extract_and_insert_posisi_keuangan(excel_path, session, emitent_name):
    xls = pd.ExcelFile(excel_path)
    posisi_keuangan_df = pd.read_excel(xls, sheet_name="1210000", header=1)
    print(f"Posisi Keuangan Data (First rows):\n{posisi_keuangan_df.head()}")  # Debug

    posisi_keuangan_df = posisi_keuangan_df[['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3']].dropna()
    posisi_keuangan_df.columns = ['category', 'current_year', 'prior_year', 'statement_position']
    posisi_keuangan_df['current_year'] = posisi_keuangan_df['current_year'].apply(convert_to_float)
    posisi_keuangan_df['prior_year'] = posisi_keuangan_df['prior_year'].apply(convert_to_float)

    for idx, row in posisi_keuangan_df.iterrows():
        session.add(FinancialStatement(
            emitent=emitent_name,
            grup_lk='Posisi Keuangan',
            item=row['category'] if row['category'] else "N/A",
            value=int(row['current_year']) if row['current_year'] else 0,
            quarter="Q1",
            notes=None
        ))

# Fungsi utama
def main():
    excel_path = r"data\FinancialStatement-2023-Tahunan-AMMS.xlsx"
    pdf_path = r"data\PT Agung Menjangan Mas Tbk 31 Des 2023 (1).pdf"
    db_url = 'mysql+pymysql://root:@localhost:3307/financial'

    # Koneksi database
    engine = create_engine("mysql+pymysql://root:@localhost:3307/financial")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Ambil nama entitas dari sheet 1000000
    emitent_name = extract_emitent_name(excel_path)

    try:
        extract_and_insert_laba_rugi(excel_path, pdf_path, session, emitent_name)
        print("Laba Rugi data inserted.")
        
        extract_and_insert_arus_kas(excel_path, pdf_path, session, emitent_name)
        print("Arus Kas data inserted.")
        
        extract_and_insert_posisi_keuangan(excel_path, session, emitent_name)
        print("Posisi Keuangan data inserted.")

        session.commit()
        print("Data berhasil disimpan ke database.")
    except Exception as e:
        session.rollback()
        print(f"Terjadi kesalahan: {e}")
        print(traceback.format_exc()) 
    finally:
        session.close()
        print("Session closed.")

if __name__ == "__main__":
    main()
