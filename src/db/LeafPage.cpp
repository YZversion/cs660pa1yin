#include <db/LeafPage.hpp>
#include <stdexcept>

using namespace db;

LeafPage::LeafPage(Page &page, const TupleDesc &td, size_t key_index) : td(td), key_index(key_index) {
  // TODO pa2: implement
  capacity = (DEFAULT_PAGE_SIZE - sizeof(LeafPageHeader) ) / td.length();
  data = (uint8_t *)malloc(capacity * td.length());
  header = new LeafPageHeader;

}

bool LeafPage::insertTuple(const Tuple &t) {
  // TODO pa2: implement
  
  int placeToInsert = 0;
  bool biggest = 1;
  bool duplicate = 0;
  int get_field_i = 0;
  for(int i = 0; i < td.size(); i++){
    if(t.field_type(i) == db::type_t::INT){
      get_field_i = i;
      break;
    }
  }
  for(int i = 0; i < header->size;i++){
    Tuple t_i = td.deserialize(data+i*td.length());
    if(t_i.get_field(get_field_i) > t.get_field(get_field_i)){
      placeToInsert = i;
      biggest = 0;
      break;
    }
    if(t_i.get_field(get_field_i) == t.get_field(get_field_i)){
      placeToInsert = i;
      duplicate = 1;
      break;
    }
  }
  if(biggest == 0 && duplicate == 0){
    for(int i = header->size; i > placeToInsert; i--){
      Tuple t_move = td.deserialize(data+(i-1)*td.length());
      td.serialize(data+i*td.length(),t_move);
    }
  }
  else if(biggest == 1 && duplicate == 0){
    placeToInsert = header->size;
  }
  td.serialize(data + placeToInsert*td.length(), t);
  if(duplicate == 0){
    header->size += 1;
  }
  if (header->size == capacity){
    return true;
  }
  return false;
}

int LeafPage::split(LeafPage &new_page) {
  // TODO pa2: implement
  header->size = capacity/2;
  new_page.header->size = capacity/2 +1;
  new_page.header->next_leaf = header->next_leaf;
  for(int i = 0; i < new_page.header->size; i++){
    Tuple t_new = td.deserialize(data+(header->size+i)*td.length());
    new_page.td.serialize(new_page.data+i*td.length(), t_new);
  }
}

Tuple LeafPage::getTuple(size_t slot) const {
  // TODO pa2: implement
  return td.deserialize(data+slot*td.length());
}
