#include <db/IndexPage.hpp>
#include <stdexcept>

using namespace db;

IndexPage::IndexPage(Page &page) {
  // TODO pa2: implement
  capacity = (DEFAULT_PAGE_SIZE - sizeof(IndexPageHeader))/(sizeof(int)+sizeof(size_t)) - 1;
  header = new IndexPageHeader;
  header->size = 0;
  keys = new int[capacity];
  children = new size_t[capacity+1];
}

bool IndexPage::insert(int key, size_t child) {
  // TODO pa2: implement
  int placeToInsert = 0;
  bool biggest = 1;
  for(int i = 0; i < header->size;i++){
    if(keys[i] > key){
      placeToInsert = i;
      biggest = 0;
      break;
    }
  }
  if(biggest == 0){
    for(int i = header->size; i > placeToInsert; i--){
      keys[i] = keys[i-1];
      children[i] = children[i-1];
    }
  }
  else{
    placeToInsert = header->size;
  }
  
  keys[placeToInsert] = key;
  children[placeToInsert] = child;
  header->size += 1;
  if (header->size == capacity){
    return true;
  }
  return false;
}

int IndexPage::split(IndexPage &new_page) {
  // TODO pa2: implement
  header->size = capacity/2;
  new_page.header->size = capacity/2-1;
  for(int i = 0; i < new_page.header->size; i++){
    new_page.keys[i] = keys[header->size+1+i];
    new_page.children[i] = children[header->size+1+i];
  }
}
