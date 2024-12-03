import pymysql

try:
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='',
        db='financial_statement',
        port=3307
    )
    print("Koneksi ke MySQL Server berhasil!")
    conn.close()
except pymysql.MySQLError as e:
    print(f"Terjadi kesalahan: {e}")
