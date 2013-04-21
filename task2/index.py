#!/bin/env python
from collections import deque
from itertools import groupby
import struct
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


# Convert docIDs to docDeltas
def to_gaps(arr):
  res = []
  last = 0
  for n in arr:
    res.append(n - last)
    last = n
  return res

# Convert docDeltas to docIDs
def from_gaps(arr):
  res = []
  last = 0
  for gap in arr:
    res.append(last + gap)
    last += gap
  return res

#
def vb_encode_num(num):
  bytes = []
  while True:
    bytes = [num % 128] + bytes
    if num < 128:
      break
    num /= 128
  bytes[len(bytes)-1] += 128
  return bytes

def vb_encode(arr):
  bytestream = []
  for n in arr:
    bytes = vb_encode_num(n)
    bytestream.extend(bytes)
  return bytestream

def vb_decode(bytes):
  numbers = []
  num = 0
  for item in bytes:
    if item < 128:
      num = 128 * num + item
    else:
      num = 128 * num + (item-128)
      numbers.append(num)
      num = 0
  return numbers

# pull one list of postings from the file
def read_posting(file):
  buf = file.read(struct.calcsize("II"))
  if len(buf) > 0:
    term_id, length = struct.unpack("II", buf)
    docID = bytearray(file.read(length))
    posting = [term_id] + from_gaps(vb_decode(docID))
    return posting
  else:
    return []


# push one list of postings to the file
def print_posting(file, posting):
  term_id = posting[0]
  docID = posting[1:]
  posting_dict[term_id] = (file.tell(), len(docID))
  content = bytearray(vb_encode(to_gaps(docID)))
  file.write(struct.pack("II", term_id, len(content)))
  file.write(content)

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

# function for merging two lines of postings list to create a new line of merged results
def merge_posting (line1, line2):
  # don't forget to return the resulting line at the end
  ans = [line1[0]]
  posting1 = deque(line1[1:])
  posting2 = deque(line2[1:])
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
  return ans


doc_id = -1
word_id = 0

for dir in sorted(os.listdir(root)):
  print >> sys.stderr, 'processing dir: ' + dir
  dir_name = os.path.join(root, dir)
  block_pl_name = out_dir+'/'+dir 
  # append block names to a queue, later used in merging
  block_q.append(dir)

  block_pl = open(block_pl_name, 'wb')
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
  term_doc_list = sorted(set(term_doc_list))
  print >> sys.stderr, 'print posting list to disc for dir:' + dir
  
  # write the posting lists to block_pl for this current block
  groups = groupby(term_doc_list, key = lambda x : x[0])
  lines = [[k] + [x[1] for x in v] for k, v in groups]
  for item in lines:
    print_posting(block_pl, item)
  block_pl.close()

print >> sys.stderr, '######\nposting list construction finished!\n##########'

print >> sys.stderr, '\nMerging postings...'

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
  comb_f = open(out_dir + '/'+comb, 'wb')

  # write the new merged posting lists block to file 'comb_f'
  f1 = read_posting(b1_f)
  f2 = read_posting(b2_f)

  while len(f1) != 0 and len(f2) != 0:
    if f1[0] == f2[0]:
      f = merge_posting(f1, f2)
      print_posting(comb_f, f)
      f1 = read_posting(b1_f)
      f2 = read_posting(b2_f)
    elif f1[0] < f2[0]:
      print_posting(comb_f, f1)
      f1 = read_posting(b1_f)
    else:
      print_posting(comb_f, f2)
      f2 = read_posting(b2_f)

  while len(f1) != 0:
    print_posting(comb_f, f1)
    f1 = read_posting(b1_f)
  while len(f2) != 0:
    print_posting(comb_f, f2)
    f2 = read_posting(b2_f)

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
