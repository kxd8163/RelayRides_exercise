#!/usr/bin/python

# Solution for RelayRides interview challange - back end part
# Author: Karina Damico
# Date: 10/11/15

import sys

class MyDatabase(object):
  #Constructor
  def __init__(self):
    #database itself is represented as dictionary (python key val data structure). In best case we have O(1)
    #access complexity
    self._db = {}
    # initialize dedicated storage to keep track fo transaction history - order matters => using list.
    # transaction history for each block is a dict capturing state of the db before it entered 
    # new block. Most recent db caprures are located at the end of the list. For space efficiency we don't 
    # store the whole capture of the db, but just diffs applied within command block
    self._history = []
  
  def begin(self):
    # creates a new 
    #self._history.insert(0, {})
    self._history.append({})

  def get(self, name):
    # check if name (key) is in db - if so print value, else print NULL
    if name in self._db:
      print self._db[name]
    else:
      print 'NULL'

  def set(self, name, val):
    # if we are currently in a transaction block - capture changes that are being made to the db into history
    # so we can undo them if requested
    if self._history:
      # update the history for items that are changed in this transaction block
      if name in self._db and name not in self._history[-1]:
        self._history[-1][name] = self._db[name]
      #case whenwe are setting new value that didn't exist before
      if name not in self._db:
        self._history[-1][name] = None
    self._set(name, val)

  def _set(self, name, val):
    # change value name for key that already exist
    if (val != None):
      self._db[name] = val
    #if we are rolling back changes - None is passed as a val, so we need to remove the corresponding 
    #key from db
    else:
      del self._db[name]

  def rollback(self):
    # read from the most recent and apply rollbacks
    if self._history:
      for key, val in self._history[-1].items():
        self._set(key, val)
      # remove the most recent db capture of the last block
      self._history.pop()
    else:
      print "Can't rollback, no state was captured..."
  
  def commit(self):
    # normally we would dump data to file or fix the new db state into some stable one
    # but for this example we just maintain self.__db as it is and empty history to indicate that
    # we are in new stable state
    self._history = []
  

  def unset(self, name):
    # set val of key (name) to "None"
    if name in self._db:
      self.set(name, None)
    

###Define main function

def main():
  db = MyDatabase()
  # define accepted commands and method to be invoked for each command
  commands = {'BEGIN' : db.begin, 'SET' : db.set, 
            'GET' : db.get, 'UNSET' : db.unset, 
            'ROLLBACK' : db.rollback, 'COMMIT': db.commit}
  #go over each instruction in thd input stream and process it
  instruction = sys.stdin.readline().strip()
  while instruction != 'END':
    args = instruction.split(' ')
    cmd = args[0]
    arguments = args[1:]
    if cmd in commands:
      #invoke appropriate method
      commands[cmd](*arguments)
    else:
      print "Illigal instruction...skipping..."
    instruction = sys.stdin.readline().strip()


if __name__ == "__main__":
  sys.exit(main())
  