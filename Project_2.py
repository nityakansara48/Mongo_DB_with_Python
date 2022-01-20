import pymongo as pm
import json
import os

txtFileList = []
jsonFileList = []
empJson = {}
deptJson = {}
projJson = {}
workJson = {}
collectionList = []
projectsCollectionList = []
employeesCollectionList = []
departmentsCollectionList = []


def DBConnection():
    try:
        port = 27017
        databaseName = 'nitya'

        client = pm.MongoClient(port=port)
        myDB = client[databaseName]

        for i in myDB.list_collection_names():
            collectionList.append(i)

        print('Connected to Database Successfully.')

        return myDB
    except Exception as e:
        print(f'DBConnection function error: {e}.')


def getNum(s):
    if not s:
        return s
    try:
        return int(s)
    except ValueError:
        return s


def Txt_to_json(filename):
    try:
        empHeader = ['Fname', 'Minit', 'Lname', 'Ssn', 'Bdate', 'Address', 'Sex', 'Salary', 'Super_ssn', 'Dno']
        deptHeader = ['Dname', 'Dnumber', 'Mgr_ssn', 'Mgr_start_date']
        projHeader = ['Pname', 'Pnumber', 'Plocation', 'Dnum']
        workHeader = ['Essn', 'Pno', 'Hours']

        jsonFileName = ''

        if filename == "EMPLOYEE.txt":
            headList = empHeader
            jsonFileName = "EMPLOYEE.json"
        elif filename == "DEPARTMENT.txt":
            headList = deptHeader
            jsonFileName = "DEPARTMENT.json"
        elif filename == "PROJECT.txt":
            headList = projHeader
            jsonFileName = "PROJECT.json"
        elif filename == "WORKS_ON.txt":
            headList = workHeader
            jsonFileName = "WORKS_ON.json"

        if jsonFileName == '':
            return 0

        txtFile = open(filename, 'r')
        jsonFile = open(jsonFileName, 'w')

        jsonData = {}
        num = 0

        for i in txtFile:
            i = i.replace("', '", "@").replace(", '", "@").replace(", ", "@").replace("',", "@").replace("'", "")
            tempList = list(i.strip().split('@'))
            tempList = [x.replace("'", "") for x in tempList]
            tempDict = {}
            tempList = list(map(getNum, tempList))
            for j in range(len(headList)):
                tempDict[headList[j]] = tempList[j]
            num += 1
            jsonData[num] = tempDict

        json.dump(jsonData, jsonFile, indent=2)

        if filename == "EMPLOYEE.txt":
            empJson.update(jsonData)
        elif filename == "DEPARTMENT.txt":
            deptJson.update(jsonData)
        elif filename == "PROJECT.txt":
            projJson.update(jsonData)
        elif filename == "WORKS_ON.txt":
            workJson.update(jsonData)

        txtFile.close()
        jsonFile.close()
    except Exception as e:
        print(f'Txt_to_json function error: {e}.')


def createJsonFiles():
    try:
        for file in os.listdir():
            if file.endswith('.txt'):
                txtFileList.append(file)
                Txt_to_json(file)

        for file in os.listdir():
            if file.endswith('.json'):
                jsonFileList.append(file)
        print('JSON files created for each Text file Successfully.')
    except Exception as e:
        print(f'createJsonFiles function error: {e}')


def createCollection(myDB):
    try:
        for i in jsonFileList:
            if i == 'EMPLOYEE.json':
                if 'employee' in myDB.list_collection_names():
                    myCollection = myDB['employee']
                    myCollection.drop()
                myCollection = myDB['employee']
                myCollection.insert_many(empJson.values())
            elif i == 'DEPARTMENT.json':
                if 'department' in myDB.list_collection_names():
                    myCollection = myDB['department']
                    myCollection.drop()
                myCollection = myDB['department']
                myCollection.insert_many(deptJson.values())
            elif i == 'PROJECT.json':
                if 'project' in myDB.list_collection_names():
                    myCollection = myDB['project']
                    myCollection.drop()
                myCollection = myDB['project']
                myCollection.insert_many(projJson.values())
            elif i == 'WORKS_ON.json':
                if 'works_on' in myDB.list_collection_names():
                    myCollection = myDB['works_on']
                    myCollection.drop()
                myCollection = myDB['works_on']
                myCollection.insert_many(workJson.values())
        print('employee, department, project and works_on collections created into Database Successfully.')
    except Exception as e:
        print(f'createCollection function error: {e}.')


def createProjectsCollection(myDB):
    try:
        myCollection = myDB['project']

        result = myCollection.aggregate([
            {
                '$lookup': {
                    'from': 'department',
                    'localField': 'Dnum',
                    'foreignField': 'Dnumber',
                    'as': 'dept'
                },
            },
            {
                '$lookup': {
                    'from': 'works_on',
                    'localField': 'Pnumber',
                    'foreignField': 'Pno',
                    'as': 'work'
                }
            },
            {
                '$lookup': {
                    'from': 'employee',
                    'localField': 'work.Essn',
                    'foreignField': 'Ssn',
                    'as': 'emp'
                }
            },
            {
                '$replaceRoot': {
                    'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$dept', 0]}, '$$ROOT']}
                }
            },
            {
                '$project': {
                    '_id': 1,
                    'Pname': 1,
                    'Pnumber': 1,
                    'Dname': 1,
                    'emp.Lname': 1,
                    'emp.Fname': 1,
                    'work.Hours': 1

                }
            }
        ])

        for i in result:
            projectsCollectionList.append(i)

        if 'projects' in myDB.list_collection_names():
            myCollection = myDB['projects']
            myCollection.drop()

        myCollection = myDB['projects']
        myCollection.insert_many(projectsCollectionList)
        print('PROJECTS collections created into Database Successfully.')
    except Exception as e:
        print(f'createProjectsCollection function error: {e}.')


def createEmployeesCollection(myDB):
    try:
        myCollection = myDB['employee']

        result = myCollection.aggregate([
            {
                '$lookup': {
                    'from': 'department',
                    'localField': 'Dno',
                    'foreignField': 'Dnumber',
                    'as': 'dept'
                },
            },
            {
                '$lookup': {
                    'from': 'works_on',
                    'localField': 'Ssn',
                    'foreignField': 'Essn',
                    'as': 'work'
                }
            },
            {
                '$lookup': {
                    'from': 'project',
                    'localField': 'work.Pno',
                    'foreignField': 'Pnumber',
                    'as': 'proj'
                }
            },
            {
                '$replaceRoot': {
                    'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$dept', 0]}, '$$ROOT']}
                }
            },
            {
                '$project': {
                    '_id': 1,
                    'Lname': 1,
                    'Fname': 1,
                    'Dname': 1,
                    'proj.Pname': 1,
                    'proj.Pnumber': 1,
                    'work.Hours': 1
                }
            }
        ])

        for i in result:
            employeesCollectionList.append(i)

        if 'employees' in myDB.list_collection_names():
            myCollection = myDB['employees']
            myCollection.drop()

        myCollection = myDB['employees']
        myCollection.insert_many(employeesCollectionList)
        print('EMPLOYEES collections created into Database Successfully.')
    except Exception as e:
        print(f'createEmployeesCollection function error: {e}.')


def createDepartmentsCollection(myDB):
    try:
        myCollection = myDB['department']

        result = myCollection.aggregate([
            {
                '$lookup': {
                    'from': 'employee',
                    'localField': 'Dnumber',
                    'foreignField': 'Dno',
                    'as': 'emp'
                },
            },
            {
                '$lookup': {
                    'from': 'employee',
                    'localField': 'Mgr_ssn',
                    'foreignField': 'Ssn',
                    'as': 'Mgr_Lname'
                },
            },
            {
                '$replaceRoot': {
                    'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$Mgr_Lname', 0]}, '$$ROOT']}
                }
            },
            {
                '$project': {
                    '_id': 1,
                    'Dname': 1,
                    'Lname': 1,
                    'Mgr_start_date': 1,
                    'emp.Lname': 1,
                    'emp.Fname': 1,
                    'emp.Salary': 1,
                }
            }
        ])

        for i in result:
            departmentsCollectionList.append(i)

        if 'departments' in myDB.list_collection_names():
            myCollection = myDB['departments']
            myCollection.drop()

        myCollection = myDB['departments']
        myCollection.insert_many(departmentsCollectionList)
        print('DEPARTMENTS collections created into Database Successfully.')
    except Exception as e:
        print(f'createDepartmentsCollection function error: {e}.')


def sampleQueries(myDB):
    try:
        myCollection = myDB['employees']

        sampleQuery = "Query 1. Details from EMPLOYEES collection whose name start with 'J' and works in 'Software' department."
        result = myCollection.find({
            'Fname': {'$regex': 'J'},
            'Dname': 'Software'
        })

        f = open('sampleQueryOutput.txt', 'w')
        f.write(sampleQuery + '\n\n')
        for i in result:
            f.write(str(i) + '\n\n')

        myCollection = myDB['projects']

        sampleQuery = "Query 2. Details from PROJECTSS colelction whose name start with 'J' and works in 'Software' department."
        result = myCollection.find({
            'Pname': 'DataMining',
            'Dname': 'Software'
        })

        f.write('\n' + sampleQuery + '\n\n')
        for i in result:
            f.write(str(i) + '\n\n')
        f.close()
    except Exception as e:
        print(f'sampleQueries function error: {e}.')


if __name__ == '__main__':
    try:
        myDB = DBConnection()
        createJsonFiles()
        createCollection(myDB)
        createProjectsCollection(myDB)
        createEmployeesCollection(myDB)
        createDepartmentsCollection(myDB)
        sampleQueries(myDB)
    except Exception as e:
        print(f'Main function exception: {e}.')