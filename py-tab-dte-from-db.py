# simple Tableau Data Extract creation from a single microsoft sql server sql statement

import dataextract as tde # saves some typing, cause i'm a lazy fucker
import os, time, pymssql # for file manipulation, script timing (not necc), database access!

###################### FOR YOUR PARAMETERS, SON! ######################
 tdefilename = 'ufo_datas.tde'
 sql = "select * from UFOdata.dbo.Sightings" # whatever
 sqlserverhost = 'localhost'
 sqlusername = 'sa'
 sqlpassword = 'passy'
 sqldatabase = 'UFOdata'
 rowoutput = False # for DEBUGGING data errors / slows shit down 10X however
###################### FOR YOUR PARAMETERS, SON! ######################

 dotsevery = 75

 start_time = time.time() # simple timing for test purposes

 mssql_db = pymssql.connect(host=sqlserverhost, user=sqlusername, password=sqlpassword, database=sqldatabase, as_dict=True) # as_dict very important
 mssql_cursor = mssql_db.cursor()
 mssql_cursor.execute(sql)

print ' '
print '[ Note: Each . = ' +str(dotsevery)+ ' rows processed ]'

 fieldnameslist = [] # define our empty list

#go through the first row to TRY to set fieldnames and datatypes
for row in mssql_cursor:
         itemz = len(row.keys())/2 # because the dict rowset includes BOTH number keys and fieldname keys
         for k in row.keys():
             fieldnameslist.append(str(k) + '|' + str(type(row[k])).replace("<type '","").replace("'>","").replace("<class '","").replace('NoneType','str').replace('uuid.UUID','str') )
         break # after the first row, we SHOULD have a decent idea of the datatypes
# ^ a bit inelegant, but it gets the job done

 fieldnameslist.sort() # sort them out so the integer keys are first (we're gonna whack em)
del fieldnameslist[0:itemz] # remove first x amount of keys (should be all integers instead of dict literals)


try:  # Just in case the file exists already, we don't want to bomb out
     tdefile = tde.Extract(tdefilename) # in CWD
except: 
     os.system('del '+tdefilename)
     os.system('del DataExtract.log') #might as well erase this bitch too
     tdefile = tde.Extract(tdefilename)

# ok lets build the table definition in TDE with our list of names and types first
# replacing literals with TDE datatype integers, etc
 tableDef = tde.TableDefinition() #create a new table def

if rowoutput == True:
     print '*** field names list ***' # debug 
for t in fieldnameslist:
     fieldtype = t.split('|')[1]
     fieldname = t.split('|')[0]
     fieldtype = str(fieldtype).replace("str","15").replace("datetime.datetime","13").replace("int","7").replace("decimal.Decimal","10").replace("float","10").replace("uuid.UUID","15").replace("bool","11")
     if rowoutput == True:
         print fieldname + '  (looks like ' + t.split('|')[1] +', TDE datatype ' + fieldtype + ')'  # debug 
     try:
         tableDef.addColumn(fieldname, int(fieldtype)) # if we pass a non-int to fieldtype, it'll fail
     except:
         tableDef.addColumn(fieldname, 15) # if we get a weird type we don't recognize, just make it a string
if rowoutput == True:
     print '***'
     time.sleep(5) # wait 5 seconds so you can actually read shit!

if rowoutput == True:
     # ok, lets print out the table def we just made, for shits and giggles
     print '################## TDE table definition created ######################'
     for c in range(0,tableDef.getColumnCount()):
         print 'Column: ' + str(tableDef.getColumnName(c)) + ' Type: ' + str(tableDef.getColumnType(c))
     time.sleep(5) # wait 5 seconds so you can actually read shit!

# ok lets add the new def as a table in the extract
 tabletran = tdefile.addTable("Extract",tableDef) 
# why table NEEDS to be called 'Extract' is beyond me

 rowsinserted = 1 # we need to count stuff, dude! Robots start at 0, I START AT 1!

# ok, for each row in the result set, we iterate through all the fields and insert based on datatype
for row in mssql_cursor:
     if rowoutput == True:
         print '************** INSERTING ROW NUMBER: ' + str(rowsinserted) + '**************' # debug output
     else: # only print dot every 50 records
         if (rowsinserted%dotsevery) == 0:
             print '.',

     columnposition = 0
     newrow = tde.Row(tableDef)
     for t in fieldnameslist:
         fieldtype = t.split('|')[1]
         fieldname = t.split('|')[0]

         if rowoutput == True:
             print str(columnposition) + ' ' + fieldname + ':   ' + str(row[fieldname]) + ' (' + str(fieldtype).split('.')[0] + ')' # debug output
         
         if fieldtype == 'str':
             if row[fieldname] != None: # we don't want no None!
                 newrow.setCharString(columnposition, str(row[fieldname]))
             else:
                 newrow.setNull(columnposition) # ok, put that None here

         if fieldtype == 'int':
             if row[fieldname] != None:
                 newrow.setInteger(columnposition, row[fieldname])
             else:
                 newrow.setNull(columnposition)

         if fieldtype == 'bool':
             if row[fieldname] != None:
                 newrow.setBoolean(columnposition, row[fieldname])
             else:
                 newrow.setNull(columnposition)

         if fieldtype == 'decimal.Decimal':
             if row[fieldname] != None:
                 newrow.setDouble(columnposition, row[fieldname])
             else:
                 newrow.setNull(columnposition)

         if fieldtype == 'datetime.datetime': # sexy datetime splitting
             if row[fieldname] != None:
                 strippeddate = str(row[fieldname]).split('.')[0] # just in case we get microseconds (not all datetime uses them)
                 timechunks = time.strptime(str(strippeddate), "%Y-%m-%d %H:%M:%S") # chunky style!
                 newrow.setDateTime(columnposition, timechunks[0], timechunks[1], timechunks[2], timechunks[3], timechunks[4], timechunks[5], 0000)
             else:
                 newrow.setNull(columnposition)
     
         columnposition = columnposition + 1 # we gots to know what column number we're working on!
     tabletran.insert(newrow) # finally insert buffered row into TDE 'table'
     newrow.close()
     rowsinserted = rowsinserted + 1

# ok let's write out that file and get back to making dinner
 tdefile.close()
 mssql_db.close()

# timing purposes for debugging / optimizing / FUN! This is FUN, Lars.
 timetaken = time.time() - start_time
print str(rowsinserted) + ' rows inserted in ' + str(timetaken) + ' seconds'
print '    (' + str(rowsinserted/timetaken) + ' rows per second)'
# woo, let's have a drink!