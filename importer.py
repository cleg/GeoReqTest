from csv import reader
from psycopg2 import connect


def _main():
    """
    Import data from http://download.geonames.org/export/dump/ allCountries list
    """
    query = "INSERT INTO \"public\".\"_CITIES\" (\"name\", \"lat\", \"long\", \"region\") " \
            "VALUES (%s, %s, %s, %s)"

    conn = connect(host='localhost', user=DB_USER, password=DB_PASS, database='geotest')

    with open("allCountries.txt") as in_f:
        with conn.cursor() as cursor:
            csv_reader = reader(in_f, delimiter='\t')
            for num, line in enumerate(csv_reader):
                ascii_name = line[2]
                lat = line[4]
                long = line[5]
                country = line[8]
                cursor.execute(query, (ascii_name, lat, long, country))
                if num % 10000 == 0:
                    conn.commit()
                    print(num)
            else:
                print("Finally")
                conn.commit()


if __name__ == "__main__":
    _main()
