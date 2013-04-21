#!/bin/env python
from collections import deque
from itertools import groupby
import os, glob, os.path
import sys
import re

if len(sys.argv) != 3:
  print >> sys.stderr, 'usage: python index.py data_dir output_dir' 
  os._exit(-1)

total_file_count = 0
root = sys.argv[1]
out_dir = sys.argv[2]
if not os.path.exists(out_dir):
  os.makedirs(out_dir)

# this is the actual posting lists dictionary
# word id -> {position_in_file, doc freq}
posting_dict = {}
# this is a dict holding document name -> doc_id
doc_id_dict = {}
# this is a dict holding word -> word_id
word_dict = {}
# this is a queue holding block names, later used for merging blocks
block_q = deque([])

# parse word id from one line of information read from the blocks
def parse_word_id(line):
  return int(line.split()[0])

# parse postings from one line of information read from the blocks
def parse_posting(line):
  return [int(u) for u in line.split()[1:] ]

# pop out postings
def popLeftOrNone(p):
  if len(p) > 0:
    posting = p.popleft()
  else:
    posting = None
  return posting

# function to count number of files in collection
def count_file():
  global total_file_count
  total_file_count += 1
  return total_file_count

# function for printing a line in a postings list to a given file
def print_posting(file, posting_line):
  # a useful function is f.tell(), which gives you the offset from beginning of file
  # you may also want to consider storing the file position and doc frequence in posting_dict in this call
  global posting_dict
  word_id = parse_word_id(posting_line)
  posting_dict[word_id] = (file.tell(), len(parse_posting(posting_line)))
  file.write(posting_line + '\n')

# function for merging two lines of postings list to create a new line of merged results
def merge_posting (line1, line2):
  # don't forget to return the resulting line at the end
  ans = []
  posting1 = deque(parse_posting(line1))
  posting2 = deque(parse_posting(line2))
  pp1 = popLeftOrNone(posting1)
  pp2 = popLeftOrNone(posting2)
  while pp1 is not None and pp2 is not None:
    if pp1 == pp2:
      ans.append(pp1)
      pp1 = popLeftOrNone(posting1)
      pp2 = popLeftOrNone(posting2)
    elif pp1 < pp2:
      ans.append(pp1)
      pp1 = popLeftOrNone(posting1)
    else:
      ans.append(pp2)
      pp2 = popLeftOrNone(posting2)
  while pp1 is not None:
    ans.append(pp1)
    pp1 = popLeftOrNone(posting1)
  while pp2 is not None:
    ans.append(pp2)
    pp2 = popLeftOrNone(posting2)
  return str(parse_word_id(line1))+' '+" ".join([str(u) for u in ans])


doc_id = -1
word_id = 0

for dir in sorted(os.listdir(root)):
  print >> sys.stderr, 'processing dir: ' + dir
  dir_name = os.path.join(root, dir)
  block_pl_name = out_dir+'/'+dir 
  # append block names to a queue, later used in merging
  block_q.append(dir)
  block_pl = open(block_pl_name, 'w')
  term_doc_list = []
  for f in sorted(os.listdir(dir_name)):
    count_file()
    file_id = os.path.join(dir, f)
    doc_id += 1
    doc_id_dict[file_id] = doc_id
    fullpath = os.path.join(dir_name, f)
    file = open(fullpath, 'r')
    for line in file.readlines():
      tokens = line.strip().split()
      for token in tokens:
        if token not in word_dict:
          word_dict[token] = word_id
          word_id += 1
        term_doc_list.append( (word_dict[token], doc_id) )
  print >> sys.stderr, 'sorting term doc list for dir:' + dir
  # sort term doc list
  term_doc_list = list(set(term_doc_list))
  term_doc_list.sort(key = lambda x: (x[0], x[1]), reverse=False)
  print >> sys.stderr, 'print posting list to disc for dir:' + dir
  # write the posting lists to block_pl for this current block
  groups = groupby(term_doc_list, key = lambda x : x[0])
  lines = [str(k) + ' ' + " ".join(str(x[1]) for x in v) + '\n' for k, v in groups]
  block_pl.writelines(lines)
  block_pl.close()

print >> sys.stderr, '######\nposting list construction finished!\n##########'

print >> sys.stderr, '\nMerging postings...'

# if there is only one block, no merge is needed
if len(block_q) == 1:
  f = open(out_dir+'/'+block_q[0], 'r')
  while True:
    pos = f.tell()
    line = f.readline().strip()
    if len(line) == 0:
      break
    posting_dict[parse_word_id(line)] = (pos, len(parse_posting(line)))
  f.close()

# iterative pairwise merge
while True:
  if len(block_q) <= 1:
    break
  b1 = block_q.popleft()
  b2 = block_q.popleft()
  print >> sys.stderr, 'merging %s and %s' % (b1, b2)
  b1_f = open(out_dir+'/'+b1, 'r')
  b2_f = open(out_dir+'/'+b2, 'r')
  comb = b1+'+'+b2
  comb_f = open(out_dir + '/'+comb, 'w')
  
  # write the new merged posting lists block to file 'comb_f'
  f1 = b1_f.readline().strip()
  f2 = b2_f.readline().strip()
  while len(f1) != 0 and len(f2) != 0:
    if parse_word_id(f1) == parse_word_id(f2):
      f = merge_posting(f1, f2)
      print_posting(comb_f, f)
      f1 = b1_f.readline().strip()
      f2 = b2_f.readline().strip()
    elif parse_word_id(f1) < parse_word_id(f2):
      print_posting(comb_f, f1)
      f1 = b1_f.readline().strip()
    else:
      print_posting(comb_f, f2)
      f2 = b2_f.readline().strip()
  while len(f1) != 0:
    print_posting(comb_f, f1)
    f1 = b1_f.readline().strip()
  while len(f2) != 0:
    print_posting(comb_f, f2)
    f2 = b2_f.readline().strip()

  b1_f.close()
  b2_f.close()
  comb_f.close()
  os.remove(out_dir+'/'+b1)
  os.remove(out_dir+'/'+b2)
  block_q.append(comb)
print >> sys.stderr, '\nPosting Lists Merging DONE!'

# rename the final merged block to corpus.index
final_name = block_q.popleft()
os.rename(out_dir+'/'+final_name, out_dir+'/corpus.index')

# print all the dictionary files
doc_dict_f = open(out_dir + '/doc.dict', 'w')
word_dict_f = open(out_dir + '/word.dict', 'w')
posting_dict_f = open(out_dir + '/posting.dict', 'w')
print >> doc_dict_f, '\n'.join( ['%s\t%d' % (k,v) for (k,v) in sorted(doc_id_dict.iteritems(), key=lambda(k,v):v)])
print >> word_dict_f, '\n'.join( ['%s\t%d' % (k,v) for (k,v) in sorted(word_dict.iteritems(), key=lambda(k,v):v)])
print >> posting_dict_f, '\n'.join(['%s\t%s' % (k,'\t'.join([str(elm) for elm in v])) for (k,v) in sorted(posting_dict.iteritems(), key=lambda(k,v):v)])
doc_dict_f.close()
word_dict_f.close()
posting_dict_f.close()

print total_file_count
