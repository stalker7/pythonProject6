import psycopg2



def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS clients(
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(40) NOT NULL,
        last_name VARCHAR(60) NOT NULL,
        email VARCHAR(60) UNIQUE NOT NULL)""");

        cur.execute("""CREATE TABLE IF NOT EXISTS phones(
        client_id INTEGER NOT NULL REFERENCES client(id) ON DELETE CASCADE,
        phone VARCHAR(40) UNIQUE NOT NULL,
        active BOOLEAN DEFAULT TRUE,
        CONSTRAINT pk PRIMARY KEY (client_id, phone));""")



def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO clients(first_name, last_name, email)
        VALUES (%s, %s, %s) RETURNING id;""",
                    (first_name, last_name, email))
        client_id = cur.fetchone()[0]
        print(f'client added {client_id}')
        if phones:
            for phone in phones:
                add_phone(conn, client_id, phone)


def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO phones(client_id, phone)
        VALUES (%s, %s);
        """, (client_id, phone))
        print(f'client {client_id} number added: {phone}')


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    change_dict = {'first_name': first_name, 'last_name': last_name, 'email': email}
    sh_set =', '.join([f"'{k}' = {v} " for k, v in change_dict.items() if v])
    sh_reqest = f"""
    UPDATE clients SET {sh_set}
    WHERE id = {client_id};"""
    with conn.cursor() as cur:
        if sh_set:
            cur.execute(sh_reqest)
        if phones:
            cur.execute("""SELECT phone FROM phones
            WHERE client_id=%s AND active=TRUE;""",
                       (client_id,))
            client_phones = cur.fetchall()
            if len(client_phones) == len(phones):
                for i, phone in enumerate(client_phones):
                    cur.execute("""
                    UPDATE phones SET phone=%s
                    WHERE client_id=%s AND active=TRUE;""",
                                (phones[i], client_id, phone[0]))
    print(f'data changes {client_id}')


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT phone FROM phones
        WHERE client_id=%s AND phone=%s;
        """, (client_id, phone))
        client_phones = cur.fetchall()
        if client_phones:
            cur.execute("""UPDATE phones SET active=FALSE
            WHERE client_id=%s AND phone=%s;""",
                        (client_id, phone))
            print(f'phone {phone} client {client_id} delete')
        else:
            print(f'phone {phone} from the client {client_id} there is no number')

def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""DELETE FROM clients WHERE id=%s;""",
                    (client_id,))
        print(f'client {client_id} delete')


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    fi_dict = {'first_name': first_name, 'last_name': last_name, 'email': email, 'phone': phone}
    fi_where = " AND ".join([f"{k} = '{v}'" for k, v in fi_dict.items() if v])
    fi_reqest = f"""SELECT DISTINCT 
    c.id, c.first_name, c.last_name FROM phones AS p
    JOIN clients AS c ON p.client_id = c.id
    WHERE {fi_where};"""
    with conn.cursor() as cur:
        cur.execute(fi_reqest)
        print(f'found {cur.fetchall()}')




with psycopg2.connect(database="work", user="postgres", password="defender") as conn:
    try:
        create_db(conn)
        add_client(conn, first_name='Владимир', last_name='Петров',email='petrov@ya.ru',
                   phones=('+79034567777','+79164567777'))
        add_client(conn, first_name='Иван', last_name='Иванов', email='ivanov1@ya.ru',
                   phones=('+79168885555',))
        add_client(conn, first_name='Семен', last_name='Семенов', email='semenov@ya.ru')

        add_phone(conn, client_id=3, phone='+79154567890')
        add_phone(conn, client_id=2, phone='+79031234560')
        delete_phone(conn, client_id=1, phone='+79034567777')
        change_client(conn, client_id=2, last_name='Сидоров', email='sidor@ya.ru', phones=('53'))
        delete_client(conn, client_id=3)
        find_client(conn, first_name='Иван', last_name='Сидоров')
    except psycopg2.Error as er:
        print(f'ERROR: {er}')


conn.close()








