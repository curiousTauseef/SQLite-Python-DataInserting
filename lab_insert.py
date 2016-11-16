"""
    122COM - Introduction to Algorithms
    lab_insert.py
    Purpose: This program allows you to insert multiple records of data from
             specific columns in a file into 'staff' table of 'firefly.sqlite'
             database. In just one go.
             
             The correct syntax to execute is:
             python3 lab_insert.py FILENAME from_column:till_column
             
             Further details about this program and an example are included in README.pdf
        
    Author : Rithin Chalumuri
    Version: 1.1 
    Date   : 15/07/16
    
"""

import sys
import sqlite3 as sql

def getFileName():
  """ 
  Function to get the file name from the first argument passed in command line.
  
  Returns:
      filename; name of the file user wants to read in data from.
      
  Raises:
      FileNotFoundError; If the entered file does not exist in the directory.
      IndexError; If the user does not pass the file name in command line.
      Exception; In case of any other unexpected errors. Eg, File Permisions.
  """
  try:      
      filename = sys.argv[1]       
      file = open(filename, "r")   # This step is just to check if file exists and if it opens or not.
      file.close()
      return filename              
    
  except FileNotFoundError:      
      print("The file '"+filename+"' does not exist in the directory. Please try again.")
      sys.exit()
      
  except IndexError:                  
      print("You have not entered the name of the file you want to read in data from. Please try again.")
      print("For any help, refer to the README.pdf file.")
      sys.exit()
      
  except:      
      print(filename + " cannot be opened.")
      sys.exit()

def getColumnRange(filename):
  """ 
  Function to get the specific range of columns the user wants to insert from reading in data file. 
  
  Parameters:
      filename (string); the name of the file user wants to read in data from.
      
  Returns:
      A tuple with start and end values (int); start is the starting column number you want include data from
      and end is the ending column number you want to include data till.
      
  Raises:
      TypeError; If the user enters alpahbets or symbols as the column numbers
      Exception; If in case there is any other unexpected error. Eg; if Data file is not properly formated.
  """
  
  try:
    file = open(filename,"r")                    # This step is used to assist the user in entering correct columns, if an error is made.
    line = (file.readlines()[0])
    line = line.strip('\n')
    data = line.split(',')
    limit = len(data)                            # Limit is the number of columns in the data file
    file.close()
    
    if (len(sys.argv[1:]) == 1):                 # If the user does not pass the 2nd argument(column range) in command line 
      start = 1-1                                # Uses the starting column as 1 (Default)
      end = 3                                    # and ending column as 3. (1:3) because newcrew.csv has 4 columns are the first 3 are required.
      return (start, end)                        
    else:
      dataRange = (sys.argv[2])                  
      dataRange = dataRange.split(':')           
      start = int(dataRange[0]) - 1              
      end = int(dataRange[1])
      
    if len(dataRange) > 2:                       # If unnecessary additional arguments are passed
      print("The extra arguments given in command line are ignored. Only the required ones are taken.")
    
    if( (end > limit) or (end < 0) or (start > limit) or (start<0) ):      # Additional tests to see if the entered values are logically correct. 
      print("Your read in file has "+str(limit)+" columns of data"+str(data)+".Please select an appropriate range and try again.")
      sys.exit()
    
    elif (end - start != 3):      #It should be 3, because the staff table needs 3 columns of data. Logical Test.
      print("You will need only 3 columns of data from read in file. Please selet an appropriate range.")
      sys.exit()
    
    else:                        
      return (start,end)
  
  except TypeError:   
    print("Please enter appropriate range of column NUMBERS and try again.")
    sys.exit()
  
  except:          
    print("Error in getting the range of columns of data from reading in file.")
    print("For any help, refer to the README.pdf file.")
    sys.exit()
    
                          
def getData(filename, start, end):
  """ 
  Function to get the data from file into the format SQL requires, which is passing values in a tuple. 
  
  Parameters:
      filename (string); the name of the file user wants to read in data from.
      start(int); is the column number user wants to start including from (including this column)
      end(int); is the column number user wants to include till (including this column)
      
  Returns:
      newData(list); A list of tuples. In which each tuple contains single record's 
                     information. 
      
  Raises:
      Exception; If in case there is any other unexpected error. 
                 Example, if Data file is not properly formated.
  """
    
  try:
    file = open(filename,"r")
    newData = []                                    
    for line in file:
      line = line.strip('\n')
      record = line.split(',')[start:end]           
      for j in range(0,end - start):                # To make sure all the NULL values, if any, are changed to None.
        if record[j] == "":                         # so that the tuple is formated to look like ("a","b",None)
          record[j] = None 
      record = tuple(record)
      newData.append(record)
    file.close()
    newData.pop(0)                                  # Removing the first entry because it contains only column headings which are not required.
    return newData
  
  except:                                             
    print("Error in getting the data from reading in file.")
    print("For any help, refer to the README.pdf file.")
    sys.exit()
    

def addData(data,sqliteFile):
  """ 
  Function to add the data from list into a specific table in a database. 
  
  Parameters:
      data(list); List of tuples ready to passed on to the sql's executemany function. 
      sqliteFile(string); the name of the sqlite database. 
  """
  
  try:
    con = sql.connect(sqliteFile)
    cur = con.cursor()
    cur.executemany('''INSERT INTO staff (forename, surname, job) VALUES(?,?,?);''',data)
    print("Data from choosen file has succesfully been inserted into '"+sqliteFile+"' database.")
        
  except sql.Error as e:
    print("Error %s:" % e.args[0])
    sys.exit()
        
  finally:
    con.commit()
    con.close()

def main():
  """ This function is used to execute all the functions in program in a proper sequence."""
  
  sqliteFile = 'firefly.sqlite'                       
  filename = getFileName()                      
  (start,end) = getColumnRange(filename)        
  data = getData(filename,start,end)            
  addData(data,sqliteFile)                      
  

    
if __name__ == '__main__':
    sys.exit(main())

  
  
