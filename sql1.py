import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="kavitha",
  database="myd"
)

mycursor = mydb.cursor()

mycursor.execute("SELECT * FROM customers2")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
