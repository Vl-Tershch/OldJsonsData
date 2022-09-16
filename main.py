import os
import json
from SecretUser import SecretUser
from mysql.connector import connect, Error

def getConnection():
    try:
        with connect(
                host = SecretUser.host,
                user = SecretUser.user,
                password = SecretUser.password,
                database = SecretUser.database,
        ) as connection:
            print('Script starts!')
            res_set = getDataFromDatabase(connection)
            print('Data from database:', res_set)

            res_data = getDataFromJSON(res_set)
            print('Data from JSONs to database:', res_data)

            uploadDataToDatabase(connection, res_data)
            print('Script ended!')
    except Error as e:
        print(e)

def getDataFromDatabase(connection):
        result_id = []
        cursor = connection.cursor(buffered=True)
        cursor.execute("SELECT ИД_ИСУ FROM уп")
        result_set = cursor.fetchall()
        for row in result_set:
            result_id.append(row[0])
        return result_id

def getDataFromJSON(ids):
    findedNumbers = []
    dataToAdd = []
    fds = sorted(os.listdir('/Users/vladtereshch/PycharmProjects/OldJsonsData/jsons/'))
    for i in fds:
        if i.endswith('.json'):
            filename = i.split('.')
            numberFile = filename[0].split('_')
            if int(numberFile[0]) in ids:
                findedNumbers.append(int(numberFile[0]))
                with open('/Users/vladtereshch/PycharmProjects/OldJsonsData/jsons/' + i) as f:
                    templates = json.load(f)
                    if 'error_code' in templates.keys():
                        dataToAdd.append((templates['result']['selection_year'], float(templates['result']['training_period']), numberFile[0]))
                    else:
                        dataToAdd.append((templates['selection_year'], float(templates['training_period']), numberFile[0]))
    return dataToAdd

def uploadDataToDatabase(connection, data):
    insert_jsons_query = """
        UPDATE уп
        SET Год = %s, Период = %s 
        WHERE ИД_ИСУ = (%s)
        """
    cursor = connection.cursor(buffered=True)
    cursor.executemany(insert_jsons_query, data)
    connection.commit()

if __name__ == '__main__':
    getConnection()
