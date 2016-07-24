# really simple TDE creation test

import dataextract as tde #saves some typing
import os, time # for file manipulation / script timing (not necc)

 start_time = time.time() # simple timing for test purposes

try:  # Just for testing purposes and re-running
     tdefile = tde.Extract('test.tde') #in CWD
except: 
     os.system('del test.tde')
     os.system('del DataExtract.log')
     tdefile = tde.Extract('test.tde')

 tableDef = tde.TableDefinition() #create a new table def

# using integers for now because the literal defs are not in the python module code
 tableDef.addColumn("field_type7", 7)   #INTEGER
 tableDef.addColumn("field_type10", 10) #DOUBLE
 tableDef.addColumn("field_type11", 11) #BOOLEAN
 tableDef.addColumn("field_type12", 12) #DATE
 tableDef.addColumn("field_type13", 13) #DATETIME
 tableDef.addColumn("field_type14", 14) #DURATION
 tableDef.addColumn("field_type15", 15) #CHAR_STRING
 tableDef.addColumn("field_type16", 16) #UNICODE_STRING

# ok, lets print out the table def, just for shits and giggles
for c in range(0,tableDef.getColumnCount()):
     print 'Column: ' + str(tableDef.getColumnName(c)) + ' Type: ' + str(tableDef.getColumnType(c))

# lets add the new def as a table in the extract
 tabletran = tdefile.addTable("Extract",tableDef) 
# why table NEEDS to be called 'Extract' is beyond me

 rowsinserted = 1

# let's create a bunch of rows! wheeeeee!
for i in range(1,1000000):

     newrow = tde.Row(tableDef)
     #newrow.setNull(0) #column
     newrow.setInteger(0,i) #column, value
     newrow.setDouble(1,i*1.4) #column, value
     newrow.setBoolean(2,1) #column, value (1/0)
     newrow.setDate(3, 2012, 12, 2) #column, y,m,d
     newrow.setDateTime(4, 2012, 12, 2, 18, 25, 55, 0000) #column, y,m,d,h,m,s,frac
     newrow.setDuration(5, 6, 18, 25, 55, 0000) #column, d,h,m,s,frac
     newrow.setCharString(6, 'Character String') #column, 'value'
     newrow.setString(7, 'Unicode String!') #column, 'value'
     rowsinserted = rowsinserted + 1 #count this row!

     tabletran.insert(newrow) #insert row into TDE table
     newrow.close()

 tdefile.close()

 timetaken = time.time() - start_time #just for timing and fun
print str(rowsinserted) + ' rows inserted in ' + str(timetaken) + ' seconds'
print '    (' + str(rowsinserted/timetaken) + ' rows per second)'