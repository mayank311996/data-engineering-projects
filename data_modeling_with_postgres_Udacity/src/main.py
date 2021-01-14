from create_tables import main as create_tables_main
from etl import main as etl_main


##############################################################################
if __name__ == '__main__':
    create_tables_main()
    print("Tables created successfully")
    etl_main()
    print("data inserted successfully")