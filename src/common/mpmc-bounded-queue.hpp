
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

// This is free and unencumbered software released into the public domain.

// Anyone is free to copy, modify, publish, use, compile, sell, or
// distribute this software, either in source code form or as a compiled
// binary, for any purpose, commercial or non-commercial, and by any
// means.

// In jurisdictions that recognize copyright laws, the author or authors
// of this software dedicate any and all copyright interest in the
// software to the public domain. We make this dedication for the benefit
// of the public at large and to the detriment of our heirs and
// successors. We intend this dedication to be an overt act of
// relinquishment in perpetuity of all present and future rights to this
// software under copyright law.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.

// For more information, please refer to <http://unlicense.org/>

// Implementation of Dmitry Vyukov's MPMC algorithm
// http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue


#ifndef __MPMC_BOUNDED_QUEUE_INCLUDED__
#define __MPMC_BOUNDED_QUEUE_INCLUDED__

//using namespace std;

#define DEFAULT_QUEUE_SIZE 65536

template<typename T>
class mpmc_bounded_queue_iter_t;

template<typename T>
class mpmc_bounded_queue_t
{
  friend class mpmc_bounded_queue_iter_t<T>;

public:

  mpmc_bounded_queue_t(
    std::size_t size) :
    _size(size),
    _mask(size - 1),
    _buffer(reinterpret_cast<node_t*>(new aligned_node_t[_size])),
    _head_seq(0),
    _tail_seq(0)
    {
      // make sure it's a power of 2
      assert((_size != 0) && ((_size & (~_size + 1)) == _size));
      
      // populate the sequence initial values
      for (size_t i = 0; i < _size; ++i) {
	_buffer[i].seq.store(i, std::memory_order_relaxed);
      }
    }

  mpmc_bounded_queue_t() : mpmc_bounded_queue_t(DEFAULT_QUEUE_SIZE) {}

  ~mpmc_bounded_queue_t()
    {
//      delete[] _buffer;
    }

  bool
  enqueue(
    const T& data)
    {
      // _head_seq only wraps at MAX(_head_seq) instead we use a mask to convert the sequence to an array index
      // this is why the ring buffer must be a size which is a power of 2. this also allows the sequence to double as a ticket/lock.
      size_t  head_seq = _head_seq.load(std::memory_order_relaxed);

      for (;;) {
	node_t*  node     = &_buffer[head_seq & _mask];
	size_t   node_seq = node->seq.load(std::memory_order_acquire);
	intptr_t dif      = (intptr_t) node_seq - (intptr_t) head_seq;

	// if seq and head_seq are the same then it means this slot is empty
	if (dif == 0) {
	  // claim our spot by moving head
	  // if head isn't the same as we last checked then that means someone beat us to the punch
	  // weak compare is faster, but can return spurious results
	  // which in this instance is OK, because it's in the loop
	  if (_head_seq.compare_exchange_weak(head_seq, head_seq + 1, std::memory_order_relaxed)) {
	    // set the data
	    node->data = data; // std::move(data);
	    // increment the sequence so that the tail knows it's accessible
	    node->seq.store(head_seq + 1, std::memory_order_release);
	    return true;
	  }
	}
	else if (dif < 0) {
	  // if seq is less than head seq then it means this slot is full and therefore the buffer is full
	  printf("OOPS\n");
	  return false;
	}
	else {
	  // under normal circumstances this branch should never be taken
	  head_seq = _head_seq.load(std::memory_order_relaxed);
	}
      }

      // never taken
      return false;
    }

  bool
  dequeue(
    T& data)
    {
      size_t       tail_seq = _tail_seq.load(std::memory_order_relaxed);

      for (;;) {
	node_t*  node     = &_buffer[tail_seq & _mask];
	size_t   node_seq = node->seq.load(std::memory_order_acquire);
	intptr_t dif      = (intptr_t) node_seq - (intptr_t)(tail_seq + 1);

	// if seq and head_seq are the same then it means this slot is empty
	if (dif == 0) {
	  // claim our spot by moving head
	  // if head isn't the same as we last checked then that means someone beat us to the punch
	  // weak compare is faster, but can return spurious results
	  // which in this instance is OK, because it's in the loop
	  if (_tail_seq.compare_exchange_weak(tail_seq, tail_seq + 1, std::memory_order_relaxed)) {
	    // set the output
	    data = node->data; //std::move(node->data);
	    // set the sequence to what the head sequence should be next time around
	    node->seq.store(tail_seq + _mask + 1, std::memory_order_release);
	    return true;
	  }
	}
	else if (dif < 0) {
	  // if seq is less than head seq then it means this slot is full and therefore the buffer is full
	  printf("YIKES %x %x \n",(intptr_t) node->seq, (intptr_t) tail_seq);
	  return false;
	}
	else {
	  // under normal circumstances this branch should never be taken
	  tail_seq = _tail_seq.load(std::memory_order_relaxed);
	}
      }

      // never taken
      return false;
    }
  typedef mpmc_bounded_queue_iter_t<T> iterator;

  iterator begin() const { 
    return  iterator( (mpmc_bounded_queue_t &) *this, _tail_seq );
  }

  iterator end() {
    return iterator( *this, _head_seq );
  }

  bool empty() const {
    return _tail_seq == _head_seq;
  }

  int size() const {
    if (_head_seq >= _tail_seq)
      return (_head_seq - _tail_seq);
    else
      return ((_size - _tail_seq) +
	      _head_seq);
  }

  void push_front ( T& data) {
    enqueue(data);
  }

  void push_back ( T& data) {
    enqueue(data);
  }

  void erase_and_dispose(iterator i) {
   assert (i == begin());
   dequeue(*i);
   delete &*i;
 }

private:

  struct node_t {
    T                     data;
    std::atomic<size_t>   seq;
  };

  typedef typename std::aligned_storage<sizeof(node_t), std::alignment_of<node_t>::value>::type aligned_node_t;
  typedef char cache_line_pad_t[64]; // it's either 32 or 64 so 64 is good enough

  cache_line_pad_t    _pad0;
  const size_t        _size;
  const size_t        _mask;
  node_t* const       _buffer;
  cache_line_pad_t    _pad1;
  std::atomic<size_t> _head_seq;
  cache_line_pad_t    _pad2;
  std::atomic<size_t> _tail_seq;
  cache_line_pad_t    _pad3;
  
  mpmc_bounded_queue_t(const mpmc_bounded_queue_t&) {}
  void operator=(const mpmc_bounded_queue_t&) {}
};

template<typename T>
class mpmc_bounded_queue_iter_t
{
  int mySize;
  int myIncrement;
  
  typedef mpmc_bounded_queue_iter_t<T> iterator;
  mpmc_bounded_queue_t <T> &myQueue;
  size_t myOffset;

public:

  mpmc_bounded_queue_iter_t(mpmc_bounded_queue_t<T> & queue, int size) :
    myQueue(queue),
    myOffset(size)
    {}

  T & operator*() {
    return myQueue._buffer[ myOffset].data;
  }

  T* operator->() const {
    return &myQueue._buffer[ myOffset].data;
  }
  
  iterator & operator++() {
    if (myOffset+1 > mySize)
      myOffset=0;
    else
      myOffset++;
    return *this;
  }

  iterator & operator--(int) {
    iterator temp = *this;
    *this--;
    return temp;
  }

  iterator & operator--() {
    if (myOffset == 0)
      myOffset=mySize;
    else
      myOffset-=1;
    return *this;
  }

  bool operator==(const mpmc_bounded_queue_iter_t &rhs) {
    return this->myOffset == rhs.myOffset;
  }

  bool operator!=(const mpmc_bounded_queue_iter_t &rhs) {
    return this->myOffset != rhs.myOffset;
  }

};

#endif

