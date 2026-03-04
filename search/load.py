from dotenv import dotenv_values
import duckdb
import os

# Read the .env file
env_path = os.path.expanduser("~/Documents/work/.env")
env_vars = dotenv_values(env_path)

# Access the specific variables
pointdb_host = env_vars.get("POINTDBHOST")
pointdb_username = env_vars.get("POINTDBUSERNAME")
pointdb_passwd = env_vars.get("POINTDBPASSWD")
pointdb = env_vars.get("POINTDB")
pointdb_addons = env_vars.get("POINTDBADDONS")
# Create DuckDB connection with MySQL
with duckdb.connect() as duckdb_connection:
    duckdb_connection.execute("INSTALL MYSQL; LOAD MYSQL;")
    duckdb_connection.execute(f"""
        ATTACH 'mysql://{pointdb_username}:{pointdb_passwd}@{pointdb_host}/{pointdb}' AS point_db (
            TYPE MYSQL
        )
    """)
    duckdb_connection.execute(f"""
        ATTACH 'mysql://{pointdb_username}:{pointdb_passwd}@{pointdb_host}/{pointdb_addons}' AS point_addons_db (
            TYPE MYSQL
        )
    """)

    acct_addr_coordinates = duckdb_connection.execute("""
        select * from point_addons_db.acct_addr_coordinates_v;
    """)
    df_acct_addr_coordinates = acct_addr_coordinates.df()

    bx24_location = duckdb_connection.execute("""
        select * from point_addons_db.bx24_location;
    """)
    df_bx24_location = bx24_location.df()

    bx24_location_geodata_v2 = duckdb_connection.execute("""
        select * from point_addons_db.bx24_location_geodata_v2;
    """)
    df_bx24_location_geodata_v2 = bx24_location_geodata_v2.df()
df_bx24_location_geodata_v2.rename(columns={"ufCrm11_1732803360":"lat_o", "ufCrm11_1732783301": "lng_o"}, inplace=True)
df_bx24_location_geodata_v2.to_json("df_bx24_location_geodata_v2.json", index=False)
df_acct_addr_coordinates.to_json("df_acct_addr_coordinates.json", index=False)

