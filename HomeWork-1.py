import psycopg2
from psycopg2 import sql

def create_db(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                email VARCHAR(255),
                phones VARCHAR(255)[]
            );
        """)
    conn.commit()

def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO clients (first_name, last_name, email, phones)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
        """, (first_name, last_name, email, phones))
        client_id = cursor.fetchone()[0]
    conn.commit()
    return client_id

def add_phone(conn, client_id, phone):
    with conn.cursor() as cursor:
        cursor.execute("""
            UPDATE clients
            SET phones = array_append(phones, %s)
            WHERE id = %s;
        """, (phone, client_id))
    conn.commit()

def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cursor:
        update_fields = []
        if first_name:
            update_fields.append(sql.Identifier('first_name') + sql.SQL(' = %s'))
        if last_name:
            update_fields.append(sql.Identifier('last_name') + sql.SQL(' = %s'))
        if email:
            update_fields.append(sql.Identifier('email') + sql.SQL(' = %s'))
        if phones:
            update_fields.append(sql.Identifier('phones') + sql.SQL(' = %s'))

        if update_fields:
            query = sql.SQL("""
                UPDATE clients
                SET {}
                WHERE id = %s;
            """).format(sql.SQL(', ').join(update_fields))
            cursor.execute(query, (first_name, last_name, email, phones, client_id))
    conn.commit()

def delete_phone(conn, client_id, phone):
    with conn.cursor() as cursor:
        cursor.execute("""
            UPDATE clients
            SET phones = array_remove(phones, %s)
            WHERE id = %s;
        """, (phone, client_id))
    conn.commit()

def delete_client(conn, client_id):
    with conn.cursor() as cursor:
        cursor.execute("""
            DELETE FROM clients
            WHERE id = %s;
        """, (client_id,))
    conn.commit()

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT * FROM clients
            WHERE
                first_name = %s OR
                last_name = %s OR
                email = %s OR
                %s = ANY(phones);
        """, (first_name, last_name, email, phone))
        result = cursor.fetchall()
    return result

# Пример использования функций
try:
    conn = psycopg2.connect(database="clients_db", user="postgres", password="postgres")
    create_db(conn)

    client_id = add_client(conn, "John", "Doe", "john.doe@example.com", ["123456789", "987654321"])
    print("Added client with ID:", client_id)

    add_phone(conn, client_id, "555555555")

    change_client(conn, client_id, first_name="Johnny", phones=["111111111", "222222222"])

    print("Client information after changes:", find_client(conn, first_name="Johnny"))

    delete_phone(conn, client_id, "111111111")

    print("Client information after deleting a phone:", find_client(conn, first_name="Johnny"))

    delete_client(conn, client_id)

    print("Client information after deleting the client:", find_client(conn, first_name="Johnny"))

finally:
    conn.close()
