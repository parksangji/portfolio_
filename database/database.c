#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

// Page size
#define PAGE_SIZE 4096
// Page Header size
#define PAGE_HEADER_SIZE 128
// order is 32.
#define LEAF_ORDER 3
// Internal order is 249.
#define INTERNAL_ORDER 3
// Maximum table id is 10.
#define MAX_TABLE_ID 10
#define BUFFER_SIZE 10

typedef uint64_t pagenum_t;
typedef struct record { 
   int64_t key;
   char value[120];
} record;

typedef struct header_page {
   pagenum_t freePageNumber;
   pagenum_t rootPageNumber;
   int64_t numberOfPages;
   char reserved[PAGE_SIZE - sizeof(int64_t) * 3];
} header_page;

typedef struct free_page {
   pagenum_t nextFreePageNumber;
   char notUsed[PAGE_SIZE - sizeof(int64_t)];
} free_page;

typedef struct page_header {
   pagenum_t parentPageNumber;
   int isLeaf;
   int numberOfKeys;
   char reserved[PAGE_HEADER_SIZE - (sizeof(int64_t) * 2 + sizeof(int) * 2)];
   pagenum_t rightSiblingPageNumberOrOneMorePageNumber;
} page_header;

typedef struct leaf_page {
   page_header pageHeader;
   record record[(PAGE_SIZE - sizeof(page_header)) / sizeof(record)];
} leaf_page;

typedef struct entry {
   int64_t key;
   int64_t pageNumber;
} entry;

typedef struct internal_page {
   page_header pageHeader;
   entry entry[(PAGE_SIZE - sizeof(page_header)) / sizeof(entry)];
} internal_page;

typedef union page_t {
   page_header pageHeader;
    header_page headerPage;
    free_page freePage;
    leaf_page leafPage;
    internal_page internalPage;
} page_t;

typedef struct buffer{
    page_t * frame;
    int table_id;
    pagenum_t page_num;
    int is_dirty;
    int is_pinned;
    struct buffer * next_of_LRU;
    struct buffer * prev_of_LRU;
} buffer;

typedef struct table_id{
    int file_descriptor;
    char pathname[100];
} table_id;

typedef struct table{
    struct table_id tableID[MAX_TABLE_ID];
    int next_table_id;
    int num_of_tables;
} table;

typedef struct buffer_manager{
    struct buffer * head;
    struct buffer * tail;
    int capacity;
    int num_of_buffers;
} buffer_manager;
int fd;

table * db_table;
buffer_manager * buf_manager;
char* ret_val;

void free_buf(buffer * buf);
int flush(buffer * buf);
int close_table(int table_id);
buffer * buf_read(int table_id,pagenum_t pagenum);
int init_db(int buf_num);
int shutdown_db();
int close_table(int table_id);
void init_table(int n);
int get_neighbor_index(int table_id,buffer * buf);
int delayed_merge(int table_id, buffer * buf);
void file_write_page(pagenum_t pagenum, const page_t* src);
void file_read_page(pagenum_t pagenum, page_t* dest);
void file_free_page(pagenum_t pagenum);
pagenum_t alloc_buf(int table_id);
int db_delete(int table_id,int64_t key);
int insert_into_internal_after_splitting(int table_id,buffer * parent_buf,int left_index,int64_t key, pagenum_t right_page_num, pagenum_t parent_page_num);
int insert_into_internal_page(buffer * parent_buf,int left_index,int64_t key,buffer * left_buf, pagenum_t right_page_num);
int get_left_index(buffer * parent_buf, pagenum_t pageNum);
int open_table (char *pathname);
int db_insert(int table_id, int64_t key, char *value);
int db_find(int table_id,int64_t key, char* ret_val);
pagenum_t find_leaf(int table_id,int64_t key);
record *make_record(int64_t key, char *value);
int create_root_page(int table_id,record* newRecord);
int insert_into_leaf(buffer* leaf_buf, record* newRecord, pagenum_t pagenum);
pagenum_t make_leaf(int table_id);
int insert_into_leaf_after_splitting(int table_id,buffer* leaf_buf,record* newRecord, pagenum_t PageNum);
int cut( int length );
int insert_into_parent(int table_id,buffer* left_buf,int64_t key,buffer* right_buf,pagenum_t PageNum, pagenum_t right_page_num);
int insert_into_new_root(int table_id,buffer * left_buf, int key, buffer * right_buf, pagenum_t PageNum, pagenum_t right_page_num);
pagenum_t make_internal(int table_id);

void print_leaf()
{
    page_t *headerPage = malloc(sizeof(page_t));
    file_read_page(0,headerPage);
    page_t * leaf = malloc(sizeof(page_t));
    file_read_page(headerPage->headerPage.rootPageNumber,leaf);
    while(leaf->pageHeader.isLeaf != 1)
        file_read_page(leaf->pageHeader.rightSiblingPageNumberOrOneMorePageNumber,leaf);
    while(leaf->pageHeader.rightSiblingPageNumberOrOneMorePageNumber != 0)
    {
        printf("%ld ",leaf->leafPage.record[0].key);
        file_read_page(leaf->pageHeader.rightSiblingPageNumberOrOneMorePageNumber,leaf);
    }
    printf("%ld ",leaf->leafPage.record[0].key);
    printf("\n");
    free(headerPage);
    free(leaf);
}
int main(){
    init_table(MAX_TABLE_ID);
    init_db(BUFFER_SIZE);
    char command[10];
    int64_t key;
    int table_id;
    char value[120];
    char pathname[100];
    page_t a;
    printf("open <pathname> : Open existing data file using ¡®pathname¡¯ or create one if not existed.\n");
    printf("insert <key> <value> : Insert input ¡®key/value¡¯ (record) to data file at the right place.\n");
    printf("find <key> :Find the record containing input ¡®key¡¯.\n");
    printf("delete <key> : Find the matching record and delete it if found.\n");
    printf("quit : Exit program.\n");
    printf("> ");
    //fd = open("id1", O_RDWR);
    //db_table->tableID[0].file_descriptor = fd;
    while(scanf("%s",command) != EOF)
    {
        if(strcmp(command,"open") == 0)
        {
            scanf(" %s",pathname);
            table_id = open_table(pathname);
            if(table_id == -1)
            {
                printf("failed\n");
            }
            else
            {
                printf("%s table id : %d\n",pathname,table_id);
            }
            
        }
        else if(strcmp(command,"insert") == 0)
        {
            for(int i = 1; i < 1000; i++)
            {
                db_insert(1,i,"a");
            }
            buffer * buf = buf_manager->head;
                 while(buf != NULL)
                 {
                     printf("d : %d p: %d t_i : %d p_n : %ld\n",buf->is_dirty,buf->is_pinned,buf->table_id,buf->page_num);
                     buf = buf->next_of_LRU;
                 }
                 printf("done\n");
            // scanf(" %d %ld %s",&table_id,&key,value);
            // if(db_insert(table_id,key,value)==0)
            // {
            //     printf("success\n");
            //     printf("buffer %d\n",buf_manager->num_of_buffers);
            //     buffer * buf = buf_manager->head;
            //     while(buf != NULL)
            //     {
            //         printf("d : %d p: %d t_i : %d p_n : %ld\n",buf->is_dirty,buf->is_pinned,buf->table_id,buf->page_num);
            //         buf = buf->next_of_LRU;
            //     }
            // }
            // else
            // {
            //     printf("failed\n");
            // }
            
        }
        else if(strcmp(command,"find") == 0)
        {
            scanf(" %d %ld",&table_id,&key);
            ret_val = malloc(sizeof(char)*120);
            if(db_find(table_id,key,ret_val) == 0){
                printf("%s\n",ret_val);
                buffer * buf = buf_manager->head;
                while(buf != NULL)
                {
                    printf("d : %d p: %d t_i : %d p_n : %ld\n",buf->is_dirty,buf->is_pinned,buf->table_id,buf->page_num);
                    buf = buf->next_of_LRU;
                }
            }
            else{
                printf("Not found %ld\n",key);
            }
        }
        else if(strcmp(command,"quit") == 0)
        {
            return 0;
        }
        else if(strcmp(command,"delete") == 0)
        {
            scanf(" %d %ld",&table_id,&key);
            if(db_delete(table_id,key) == 0)
            {
                printf("success\n");
                buffer * buf = buf_manager->head;
                while(buf != NULL)
                {
                    printf("d : %d p: %d t_i : %d p_n : %ld\n",buf->is_dirty,buf->is_pinned,buf->table_id,buf->page_num);
                    buf = buf->next_of_LRU;
                }
            }
            else
            {
                printf("failed\n");
            }
            
        }
        else if(strcmp(command,"close")== 0)
        {
            scanf(" %d",&table_id);
            if(close_table(table_id)==0)
            {
                printf("success\n");
            }
            else
            {
                printf("failed\n");
            }
        }
        else if(strcmp(command,"shutdown") == 0)
        {
            shutdown_db();
        }
        else if(strcmp(command,"close") == 0)
        {
            scanf(" %d",&table_id);
            close_table(table_id);
        }
        else
        {
            while (getchar() != (int)'\n');
            printf("Invalid command\n\n");
        }
        printf("> ");
    }

    return 0;
}
int open_table (char *pathname)
{
    if(db_table->num_of_tables == MAX_TABLE_ID)
    {
        return -1;
    }
    if ((fd = open(pathname, O_RDWR)) > 0){
        int i , table_id;
        for(i = 0; i < MAX_TABLE_ID; i++)
        {
            if(strcmp(db_table->tableID[i].pathname,pathname) == 0) break;
        }
        if(i == MAX_TABLE_ID)
        {
            table_id = db_table->next_table_id++;
        }
        else
        {
            table_id = i+1;
        }
        db_table->tableID[table_id-1].file_descriptor = fd;
        db_table->num_of_tables++;
        strcpy(db_table->tableID[table_id-1].pathname,pathname);
        buffer * header_buf = buf_read(table_id,0);
        if(header_buf->frame->headerPage.numberOfPages == 0)
        {
            header_buf->frame->headerPage.freePageNumber = 0;
            header_buf->frame->headerPage.numberOfPages = 1;
            header_buf->frame->headerPage.rootPageNumber = 0;
            header_buf->is_dirty = 1;
            header_buf->table_id = table_id;
            header_buf->page_num = 0;
        }
        header_buf->is_pinned = 0;
        buffer * buf = buf_manager->head;
        while(buf != NULL)
        {
            printf("d : %d p: %d t_i : %d p_n : %ld\n",buf->is_dirty,buf->is_pinned,buf->table_id,buf->page_num);
            buf = buf->next_of_LRU;
        }
        return table_id;
    }
    else if ((fd = creat(pathname, 0777)) > 0) 
    {
       close(fd);
       fd = open(pathname, O_RDWR);
       int table_id = db_table->next_table_id++;
       strcpy(db_table->tableID[table_id-1].pathname , pathname);
       db_table->tableID[table_id-1].file_descriptor = fd;
       db_table->num_of_tables++;
       buffer * header_buf = buf_read(table_id,0);
       header_buf->frame = malloc(sizeof(page_t));
       header_buf->frame->headerPage.freePageNumber = 0;
       header_buf->frame->headerPage.numberOfPages = 1;
       header_buf->frame->headerPage.rootPageNumber = 0;
       header_buf->is_dirty = 1;
       header_buf->table_id = table_id;
       header_buf->page_num = 0;
       header_buf->is_pinned = 0;
       buffer * buf = buf_manager->head;
        while(buf != NULL)
        {
            printf("d : %d p: %d t_i : %d p_n : %ld\n",buf->is_dirty,buf->is_pinned,buf->table_id,buf->page_num);
            buf = buf->next_of_LRU;
        }
       return table_id;
    }
    else {
       return -1;
    }
}
int  db_insert(int table_id, int64_t key, char *value)
{

    fd = db_table->tableID[table_id-1].file_descriptor;
    if(fd == 0)return -1;
    buffer *header_buf;
    record* newRecord;
    pagenum_t leafPageNum;
    buffer * leaf_buf;
    char * ret_val;
    ret_val = malloc(sizeof(char)*120);
    if(db_find(table_id,key,ret_val) == 0){
        printf("Aleady exists %ld\n",key);
        return -1;
    }
    newRecord = make_record(key,value);
    header_buf = buf_read(table_id,0);
    if(header_buf->frame->headerPage.rootPageNumber == 0){
        header_buf->is_pinned = 0;
        return create_root_page(table_id,newRecord);
    }
    header_buf->is_pinned = 0;
    leafPageNum = find_leaf(table_id,key);
    leaf_buf = buf_read(table_id,leafPageNum);
    if(leaf_buf->frame->pageHeader.numberOfKeys < LEAF_ORDER - 1){
        return insert_into_leaf(leaf_buf,newRecord,leafPageNum);
    }
    else{
        return insert_into_leaf_after_splitting(table_id,leaf_buf,newRecord,leafPageNum);
    }
}
int db_find(int table_id,int64_t key, char* ret_val)
{
    fd = db_table->tableID[table_id-1].file_descriptor;
    if(fd == 0)return -1;
    int i = 0;
    buffer * leaf_buf;
    pagenum_t pageNum = find_leaf(table_id,key);
    if(pageNum == 0) return -1;
    leaf_buf = buf_read(table_id,pageNum);
    for(i = 0; i < leaf_buf->frame->pageHeader.numberOfKeys; i++){
        if (leaf_buf->frame->leafPage.record[i].key == key) break;
    }
    if (i == leaf_buf->frame->pageHeader.numberOfKeys){
        leaf_buf->is_pinned = 0;
        return -1;
    }
    else
    {
        strcpy(ret_val,leaf_buf->frame->leafPage.record[i].value);
        leaf_buf->is_pinned = 0;
        return 0;
    }
}
pagenum_t find_leaf(int table_id,int64_t key)
{
    int i = 0;
    buffer * header_buf , *buf;
    header_buf = buf_read(table_id,0);
    pagenum_t pagenum = header_buf->frame->headerPage.rootPageNumber;
    if(pagenum == 0) {
        header_buf->is_pinned = 0;
        return 0;
    }
    page_t * page;
    if(header_buf->frame->headerPage.numberOfPages == 1){
        header_buf->is_pinned = 0;
        return 0;
    }
    header_buf->is_pinned = 0;
    buf = buf_read(table_id,pagenum);
    page = buf->frame;
    
    while(page->pageHeader.isLeaf != 1){
        i = 0;
        while(i < page->pageHeader.numberOfKeys){
            if(key >= page->internalPage.entry[i].key) i++;
            else break;
        }
        if(i == 0)
        {
                pagenum = page->pageHeader.rightSiblingPageNumberOrOneMorePageNumber;
                buf->is_pinned = 0;
                buf = buf_read(table_id,pagenum);
                page = buf->frame;
        }
        else{
                pagenum = page->internalPage.entry[i-1].pageNumber;
                buf->is_pinned = 0;
                buf = buf_read(table_id,pagenum);
                page = buf->frame;
        }
    }
    buf->is_pinned = 0;
    return pagenum;
}
record* make_record(int64_t key, char *value)
{
    record* new_record = malloc(sizeof(struct record));
    if (new_record == NULL){
        perror("Record creation.");
        exit(EXIT_FAILURE);
    }
    else
    {
        new_record->key = key;
        strcpy(new_record->value,value);
    }
    return new_record;  
}

int create_root_page(int table_id,record* newRecord)
{
    buffer * buf;
    page_t * root, * headerPage;
    pagenum_t root_page_num;
    root_page_num = make_leaf(table_id);
    buf = buf_read(table_id,root_page_num);
    buf->frame->pageHeader.parentPageNumber = 0;
    buf->frame->pageHeader.numberOfKeys++;
    buf->frame->leafPage.record[0].key = newRecord->key;
    strcpy(buf->frame->leafPage.record[0].value,newRecord->value);
    buf->is_pinned = 0;
    buf->is_dirty = 1;
    buf = buf_read(table_id,0);
    buf->frame->headerPage.rootPageNumber = root_page_num;
    free(newRecord);
    buf->is_pinned = 0;
    buf->is_dirty = 1;
    return 0;
}

int insert_into_leaf(buffer* leaf_buf, record* newRecord, pagenum_t pagenum)
{
    if(leaf_buf == NULL){
        return -1;
    }
    int i, insertion_point;
    insertion_point = 0;
    while(insertion_point < leaf_buf->frame->pageHeader.numberOfKeys && leaf_buf->frame->leafPage.record[insertion_point].key < newRecord->key){
            insertion_point++;
        }
    for(i= leaf_buf->frame->pageHeader.numberOfKeys; i > insertion_point; i--){
        leaf_buf->frame->leafPage.record[i] = leaf_buf->frame->leafPage.record[i-1];
    }
    leaf_buf->frame->leafPage.record[insertion_point].key = newRecord->key;
    strcpy(leaf_buf->frame->leafPage.record[insertion_point].value,newRecord->value);
    leaf_buf->frame->pageHeader.numberOfKeys++;
    free(newRecord);
    leaf_buf->is_pinned = 0;
    leaf_buf->is_dirty = 1;
    return 0;
}

pagenum_t make_leaf(int table_id)
{
    buffer * header_buf, * new_leaf_buf, *temp_buf, *prev_buf, * next_buf;
    pagenum_t new_leaf_page_num;
    header_buf = buf_read(table_id,0);
    page_t *headerPage = header_buf->frame;
    header_buf->is_pinned = 0;
    if(headerPage->headerPage.freePageNumber != 0)
    {
        return alloc_buf(table_id);
    }
    else
    {
        headerPage->headerPage.numberOfPages++;
        new_leaf_page_num = headerPage->headerPage.numberOfPages-1;
    }
    if(buf_manager->num_of_buffers == buf_manager->capacity)
    {
        temp_buf = buf_manager->head;
        while(temp_buf != NULL)
        {
            if(temp_buf->is_dirty == 1)
            {
                if(temp_buf->is_pinned == 1)break;
                buffer * write_buffer = temp_buf;
                if(buf_manager->tail == write_buffer) 
                {
                    buf_manager->tail = write_buffer->prev_of_LRU;
                    buf_manager->tail->next_of_LRU = NULL;
                    flush(write_buffer);
                    break;
                }
                else if(buf_manager->head == write_buffer)
                {
                    buf_manager->head = write_buffer->next_of_LRU;
                    buf_manager->head->prev_of_LRU = NULL;
                    flush(write_buffer);
                    break;
                }
                else
                {
                    prev_buf = write_buffer->prev_of_LRU;
                    next_buf = write_buffer->next_of_LRU;
                    prev_buf->next_of_LRU = next_buf;
                    next_buf->prev_of_LRU = prev_buf;
                    flush(write_buffer);
                    break;
                }
            }
            temp_buf = temp_buf->next_of_LRU;
        }
        if(buf_manager->num_of_buffers == buf_manager->capacity)
        {
            temp_buf = buf_manager->head;
            while(temp_buf->is_pinned == 1)
            {
                temp_buf = temp_buf->next_of_LRU;
            }
            buffer * write_buffer = temp_buf;
            if(buf_manager->tail == write_buffer) 
            {
                buf_manager->tail = write_buffer->prev_of_LRU;
                buf_manager->tail->next_of_LRU = NULL;
            } 
            else if(buf_manager->head == write_buffer)
            {
                buf_manager->head = write_buffer->next_of_LRU;
                buf_manager->head->prev_of_LRU = NULL;
            }
            else
            {
                prev_buf = write_buffer->prev_of_LRU;
                next_buf = write_buffer->next_of_LRU;
                prev_buf->next_of_LRU = next_buf;
                next_buf->prev_of_LRU = prev_buf;
            }
            flush(write_buffer);
        }
    }
    new_leaf_buf = malloc(sizeof(buffer));
    new_leaf_buf->frame = malloc(sizeof(page_t));
    new_leaf_buf->frame->pageHeader.isLeaf = 1;
    new_leaf_buf->frame->pageHeader.numberOfKeys = 0;
    new_leaf_buf->frame->pageHeader.parentPageNumber = 0;
    new_leaf_buf->is_dirty = 0;
    new_leaf_buf->is_pinned = 0;
    new_leaf_buf->page_num = new_leaf_page_num;
    new_leaf_buf->table_id = table_id;
    new_leaf_buf->next_of_LRU = NULL;
    buf_manager->tail->next_of_LRU = new_leaf_buf;
    new_leaf_buf->prev_of_LRU = buf_manager->tail;
    buf_manager->tail = new_leaf_buf;
    buf_manager->num_of_buffers++;
    header_buf->is_pinned = 0;
    header_buf->is_dirty = 1;

    return new_leaf_page_num;
}

int insert_into_leaf_after_splitting(int table_id,buffer * leaf_buf,record* newRecord, pagenum_t PageNum)
{
    buffer * new_leaf_buf;
    pagenum_t right_page_num, new_leaf_page_num; 
    record *temp_record;
    int insertion_index, split, new_key, i, j;
    new_leaf_page_num = make_leaf(table_id);
    new_leaf_buf = buf_read(table_id,new_leaf_page_num);
    if(new_leaf_buf == NULL)
    {
        return -1;
    }
    right_page_num = new_leaf_page_num;
    insertion_index = 0;
    while(insertion_index < LEAF_ORDER - 1 && leaf_buf->frame->leafPage.record[insertion_index].key < newRecord->key){
        insertion_index++;
    }
    temp_record = malloc(sizeof(record)*LEAF_ORDER);
    for(i = 0, j = 0; i < leaf_buf->frame->pageHeader.numberOfKeys; i++, j++){
        if(j == insertion_index) j++;
        temp_record[j] = leaf_buf->frame->leafPage.record[i];
    }
    temp_record[insertion_index].key = newRecord->key;
    strcpy(temp_record[insertion_index].value , newRecord->value);
    leaf_buf->frame->pageHeader.numberOfKeys = 0;
    split = cut(LEAF_ORDER - 1);

    for(i = 0; i < split; i++) {
        leaf_buf->frame->leafPage.record[i] = temp_record[i];
        leaf_buf->frame->pageHeader.numberOfKeys++;
    }
    for(i = split, j = 0; i < LEAF_ORDER; i++, j++){
        new_leaf_buf->frame->leafPage.record[j] = temp_record[i];
        new_leaf_buf->frame->pageHeader.numberOfKeys++;
    }
    leaf_buf->frame->pageHeader.rightSiblingPageNumberOrOneMorePageNumber = right_page_num;
    new_leaf_buf->frame->pageHeader.parentPageNumber = leaf_buf->frame->pageHeader.parentPageNumber;
    new_key = new_leaf_buf->frame->leafPage.record[0].key;
    free(temp_record);
    leaf_buf->is_dirty = 1;
    new_leaf_buf->is_dirty = 1;
    return insert_into_parent(table_id,leaf_buf,new_key,new_leaf_buf,PageNum,right_page_num);
}
int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}
int insert_into_parent(int table_id,buffer* left_buf,int64_t key,buffer* right_buf,pagenum_t PageNum, pagenum_t right_page_num){
    int left_index;
    pagenum_t parent_page_num;
    buffer * parent_buf;
    left_buf->is_dirty = 1;
    right_buf->is_dirty = 1;
    parent_page_num = left_buf->frame->pageHeader.parentPageNumber;
    if(parent_page_num == 0){
        return insert_into_new_root(table_id,left_buf,key,right_buf,PageNum,right_page_num);
    }
    right_buf->is_pinned = 0;
    parent_buf = buf_read(table_id,left_buf->frame->pageHeader.parentPageNumber);
    left_index = get_left_index(parent_buf,PageNum);
    if(parent_buf->frame->pageHeader.numberOfKeys < INTERNAL_ORDER - 1)
    {
        return insert_into_internal_page(parent_buf,left_index,key,left_buf,right_page_num);
    }
    left_buf->is_pinned = 0;
    return insert_into_internal_after_splitting(table_id,parent_buf,left_index,key,right_page_num, parent_page_num);
}
int insert_into_new_root(int table_id, buffer * left_buf, int key, buffer * right_buf, pagenum_t PageNum, pagenum_t right_page_num)
{
    buffer * root_buf , *header_buf;
    pagenum_t root_page_num;
    root_page_num = make_internal(table_id);
    root_buf = buf_read(table_id,root_page_num);
    if(root_buf == NULL) return -1;
    left_buf->frame->pageHeader.parentPageNumber = root_page_num;
    right_buf->frame->pageHeader.parentPageNumber = root_page_num;
    header_buf = buf_read(table_id,0);
    header_buf->frame->headerPage.rootPageNumber = root_page_num;
    root_buf->frame->internalPage.entry[0].key = key;
    root_buf->frame->pageHeader.rightSiblingPageNumberOrOneMorePageNumber = PageNum;
    root_buf->frame->internalPage.entry[0].pageNumber = right_page_num;
    root_buf->frame->pageHeader.numberOfKeys++;
    root_buf->frame->pageHeader.parentPageNumber = 0;
    
    left_buf->is_pinned = 0;
    right_buf->is_pinned = 0;
    root_buf->is_pinned = 0;
    header_buf->is_pinned = 0;
    left_buf->is_dirty = 1;
    right_buf->is_dirty = 1;
    root_buf->is_dirty = 1;
    header_buf->is_dirty = 1;

    return 0;
}
pagenum_t make_internal(int table_id)
{
    buffer * header_buf, * new_internal_buf, *temp_buf, *prev_buf, * next_buf;
    pagenum_t new_internal_page_num;
    header_buf = buf_read(table_id,0);
    page_t *headerPage = header_buf->frame;
    if(headerPage->headerPage.freePageNumber != 0)
    {
        new_internal_page_num = alloc_buf(table_id);
    }
    else
    {
        headerPage->headerPage.numberOfPages++;
        new_internal_page_num = headerPage->headerPage.numberOfPages-1;
    }
    if(buf_manager->num_of_buffers == buf_manager->capacity)
    {
        temp_buf = buf_manager->head;
        while(temp_buf != NULL)
        {
            if(temp_buf->is_dirty == 1)
            {
                if(temp_buf->is_pinned == 1)break;
                buffer * write_buffer = temp_buf;
                if(buf_manager->tail == write_buffer) 
                {
                    buf_manager->tail = write_buffer->prev_of_LRU;
                    buf_manager->tail->next_of_LRU = NULL;
                    flush(write_buffer);
                    break;
                }
                else if(buf_manager->head == write_buffer)
                {
                    buf_manager->head = write_buffer->next_of_LRU;
                    buf_manager->head->prev_of_LRU = NULL;
                    flush(write_buffer);
                    break;
                }
                else
                {
                    prev_buf = write_buffer->prev_of_LRU;
                    next_buf = write_buffer->next_of_LRU;
                    prev_buf->next_of_LRU = next_buf;
                    next_buf->prev_of_LRU = prev_buf;
                    flush(write_buffer);
                    break;
                }
            }
            temp_buf = temp_buf->next_of_LRU;
        }
        if(buf_manager->num_of_buffers == buf_manager->capacity)
        {
            temp_buf = buf_manager->head;
            while(temp_buf->is_pinned == 1)
            {
                temp_buf = temp_buf->next_of_LRU;
            }
            buffer * write_buffer = temp_buf;
            if(buf_manager->tail == write_buffer) 
            {
                buf_manager->tail = write_buffer->prev_of_LRU;
                buf_manager->tail->next_of_LRU = NULL;
            } 
            else if(buf_manager->head == write_buffer)
            {
                buf_manager->head = write_buffer->next_of_LRU;
                buf_manager->head->prev_of_LRU = NULL;
            }
            else
            {
                prev_buf = write_buffer->prev_of_LRU;
                next_buf = write_buffer->next_of_LRU;
                prev_buf->next_of_LRU = next_buf;
                next_buf->prev_of_LRU = prev_buf;
            }
            flush(write_buffer);
        }
    }
    new_internal_buf = malloc(sizeof(buffer));
    new_internal_buf->frame = malloc(sizeof(page_t));
    new_internal_buf->frame->pageHeader.isLeaf = 0;
    new_internal_buf->frame->pageHeader.numberOfKeys = 0;
    new_internal_buf->frame->pageHeader.parentPageNumber = 0;
    new_internal_buf->is_dirty = 0;
    new_internal_buf->is_pinned = 0;
    new_internal_buf->page_num = new_internal_page_num;
    new_internal_buf->table_id = table_id;
    new_internal_buf->next_of_LRU = NULL;
    buf_manager->tail->next_of_LRU = new_internal_buf;
    new_internal_buf->prev_of_LRU = buf_manager->tail;
    buf_manager->tail = new_internal_buf;
    buf_manager->num_of_buffers++;
    header_buf->is_pinned = 0;
    header_buf->is_dirty = 1;

    return new_internal_page_num;
}
int get_left_index(buffer * parent_buf, pagenum_t pageNum)
{
    int left_index = 0;
    if(parent_buf->frame->pageHeader.rightSiblingPageNumberOrOneMorePageNumber == pageNum)
    {
        return left_index;
    }
    while(left_index <= parent_buf->frame->pageHeader.numberOfKeys &&
            parent_buf->frame->internalPage.entry[left_index].pageNumber != pageNum)
    {
        left_index++;
    }

    return left_index + 1;        
}
int insert_into_internal_page(buffer * parent_buf,int left_index,int64_t key,buffer * left_buf, pagenum_t right_page_num)
{
    int i;

    for(i = parent_buf->frame->pageHeader.numberOfKeys; i > left_index - 1; i-- )
    {
        parent_buf->frame->internalPage.entry[i].key = parent_buf->frame->internalPage.entry[i-1].key;
        parent_buf->frame->internalPage.entry[i].pageNumber = parent_buf->frame->internalPage.entry[i-1].pageNumber;
    }
    parent_buf->frame->internalPage.entry[left_index].key = key;
    parent_buf->frame->internalPage.entry[left_index].pageNumber = right_page_num;
    parent_buf->frame->pageHeader.numberOfKeys++;
    parent_buf->is_pinned = 0;
    left_buf->is_pinned = 0;
    parent_buf->is_dirty = 1;
    return 0;
}
int insert_into_internal_after_splitting(int table_id,buffer * parent_buf,int left_index,int64_t key, pagenum_t right_page_num, pagenum_t parent_page_num)
{
    int i, j, split, k_prime;
    buffer * new_buf, *child_buf;
    pagenum_t new_page_num;
    entry *temp_entry;
    temp_entry = malloc(sizeof(entry)*INTERNAL_ORDER);
    for(i = 0 , j = 0; i < parent_buf->frame->pageHeader.numberOfKeys; i++ , j++)
    {
        if(j == left_index) j++;
        temp_entry[j] = parent_buf->frame->internalPage.entry[i];
    }
    temp_entry[left_index].key = key;
    temp_entry[left_index].pageNumber = right_page_num;
    split = cut(INTERNAL_ORDER-1);
    new_page_num = make_internal(table_id);
    new_buf = buf_read(table_id,new_page_num);
    if(new_buf == NULL)
    {
        return -1;
    }
    parent_buf->frame->pageHeader.numberOfKeys = 0;
    for(i = 0; i < split; i++)
    {
        parent_buf->frame->internalPage.entry[i] = temp_entry[i];
        parent_buf->frame->pageHeader.numberOfKeys++;
    }
    
    k_prime = temp_entry[split].key;
    new_buf->frame->pageHeader.rightSiblingPageNumberOrOneMorePageNumber = temp_entry[i].pageNumber;
    for(++i, j = 0; i < INTERNAL_ORDER; i++, j++)
    {
        new_buf->frame->internalPage.entry[j] = temp_entry[i];
        new_buf->frame->pageHeader.numberOfKeys++;
    }
    new_buf->frame->pageHeader.parentPageNumber = parent_buf->frame->pageHeader.parentPageNumber;
    for(int i = 0; i < new_buf->frame->pageHeader.numberOfKeys + 1; i++)
    {
        if(i == 0)
        {
            child_buf = buf_read(table_id,new_buf->frame->pageHeader.rightSiblingPageNumberOrOneMorePageNumber);
            child_buf->frame->pageHeader.parentPageNumber = new_page_num;
            child_buf->is_pinned = 0;
            child_buf->is_dirty = 1;
        }
        else
        {
            child_buf = buf_read(table_id,new_buf->frame->internalPage.entry[i-1].pageNumber);
            child_buf->frame->pageHeader.parentPageNumber = new_page_num;
            child_buf->is_pinned = 0;
            child_buf->is_dirty = 1;
        }
    }
    free(temp_entry);
    parent_buf->is_dirty = 1;
    new_buf->is_dirty = 1;
    return insert_into_parent(table_id,parent_buf,k_prime,new_buf,parent_page_num,new_page_num);
}
int db_delete(int table_id, int64_t key)
{
    fd = db_table->tableID[table_id-1].file_descriptor;
    if(fd == 0)return -1;
    int i;
    buffer * leaf_buf;
    pagenum_t leaf_page_num;
    page_t * leaf;
    ret_val = malloc(sizeof(char)*120);
    if(db_find(table_id,key,ret_val) == -1)
    {
        printf("Not found %ld\n",key);
        return -1;
    }
    leaf_page_num = find_leaf(table_id,key);
    leaf_buf = buf_read(table_id,leaf_page_num);
    leaf = leaf_buf->frame;
    i = 0;
    while(leaf->leafPage.record[i].key != key)
    {
        i++;
    }
    for(++i; i < leaf->pageHeader.numberOfKeys; i++)
    {
        leaf->leafPage.record[i - 1] = leaf->leafPage.record[i];
    }
    leaf->pageHeader.numberOfKeys--;
    leaf_buf->is_dirty = 1;
    if(leaf->pageHeader.numberOfKeys == 0)
    {
        if(leaf->pageHeader.parentPageNumber == 0)
        {
            free_buf(leaf_buf);
            buffer * header_buf = buf_read(table_id,0);
            page_t *headerPage = header_buf->frame;
            headerPage->headerPage.rootPageNumber = 0;
            header_buf->is_pinned = 0;
            header_buf->is_dirty = 1;
            return 0;
        }
        int neighbor_index = get_neighbor_index(table_id,leaf_buf);
        buffer * parent_buf, * neighbor_buf;
        page_t * parent , *neighbor;
        parent_buf = buf_read(table_id,leaf->pageHeader.parentPageNumber);
        parent = parent_buf->frame;
        if(neighbor_index == -1)
        {
            buffer * header_buf;
            header_buf = buf_read(table_id,0);
            page_t *headerPage = header_buf->frame;
            neighbor_buf = parent_buf;
            header_buf->is_pinned = 0;
            neighbor = parent_buf->frame;
            while(neighbor->pageHeader.isLeaf != 1)
            {
                neighbor_buf->is_pinned = 0;
                neighbor_buf = buf_read(table_id,neighbor->pageHeader.rightSiblingPageNumberOrOneMorePageNumber);
                if(neighbor_buf == NULL)
                {
                    return delayed_merge(table_id,leaf_buf);
                }
                neighbor = neighbor_buf->frame;
            }
            while(neighbor->pageHeader.rightSiblingPageNumberOrOneMorePageNumber != leaf_page_num)
            {
                neighbor_buf->is_pinned = 0;
                neighbor_buf = buf_read(table_id,neighbor->pageHeader.rightSiblingPageNumberOrOneMorePageNumber);
                neighbor = neighbor_buf->frame;
            }
            neighbor->pageHeader.rightSiblingPageNumberOrOneMorePageNumber = leaf->pageHeader.rightSiblingPageNumberOrOneMorePageNumber;
            neighbor_buf->is_pinned = 0;
            neighbor_buf->is_dirty = 1;
        }
        else if(neighbor_index == 0)
        {
            neighbor_buf = buf_read(table_id,parent->pageHeader.rightSiblingPageNumberOrOneMorePageNumber);
            neighbor = neighbor_buf->frame;
            neighbor->pageHeader.rightSiblingPageNumberOrOneMorePageNumber = leaf->pageHeader.rightSiblingPageNumberOrOneMorePageNumber;
            neighbor_buf->is_pinned = 0;
            neighbor_buf->is_dirty = 1;
        }
        else
        {
            neighbor_buf = buf_read(table_id,parent->internalPage.entry[neighbor_index-1].pageNumber);
            neighbor = neighbor_buf->frame;
            neighbor->pageHeader.rightSiblingPageNumberOrOneMorePageNumber = leaf->pageHeader.rightSiblingPageNumberOrOneMorePageNumber;
            neighbor_buf->is_pinned = 0;
            neighbor_buf->is_dirty = 1;
        }
        parent_buf->is_pinned = 0;
        neighbor_buf->is_pinned = 0;
        return delayed_merge(table_id,leaf_buf);
    }
    leaf_buf->is_pinned = 0;
    return 0;
}
pagenum_t alloc_buf(int table_id)
{
    pagenum_t on_disk_page_num;
    buffer * header_buf = buf_read(table_id,0);
    on_disk_page_num = header_buf->frame->headerPage.freePageNumber;
    buffer * free_buf = buf_read(table_id,on_disk_page_num);
    header_buf->frame->headerPage.freePageNumber = free_buf->frame->freePage.nextFreePageNumber;
    header_buf->is_dirty = 1;
    header_buf->is_pinned = 0;
    free_buf->is_pinned = 0;
    free_buf->is_dirty = 0;
    return on_disk_page_num;
}
void file_free_page(pagenum_t pagenum)
{
    page_t * new_free_page;
    new_free_page = malloc(sizeof(page_t));
    file_read_page(pagenum,new_free_page);
    page_t *headerPage = malloc(sizeof(page_t));
    file_read_page(0,headerPage);
    new_free_page->freePage.nextFreePageNumber = headerPage->headerPage.freePageNumber;
    headerPage->headerPage.freePageNumber = pagenum;
    file_write_page(pagenum,new_free_page);
    file_write_page(0,headerPage);
    free(headerPage);
}
void file_read_page(pagenum_t pagenum, page_t* dest)
{
    pread(fd,dest,PAGE_SIZE,PAGE_SIZE*pagenum);
}
void file_write_page(pagenum_t pagenum, const page_t* src)
{
    pwrite(fd,src,PAGE_SIZE,PAGE_SIZE*pagenum);
}
int delayed_merge(int table_id, buffer * buf)
{
    int i, j, count;
    buffer * parent_buf;
    page_t * parent;
    page_t * page = buf->frame;
    parent_buf = buf_read(table_id,page->pageHeader.parentPageNumber);
    parent = parent_buf->frame;
    if(parent == NULL) return -1;
    if(parent->pageHeader.rightSiblingPageNumberOrOneMorePageNumber == buf->page_num)
    {
        parent->pageHeader.rightSiblingPageNumberOrOneMorePageNumber = parent->internalPage.entry[0].pageNumber;
        i = 0;
        for(++i; i < parent->pageHeader.numberOfKeys; i ++)
        {
            parent->internalPage.entry[i - 1] = parent->internalPage.entry[i];
        }
        parent->pageHeader.numberOfKeys--;
    }
    else
    {
        i = 0;
        while(parent->internalPage.entry[i].pageNumber != buf->page_num)
        {
        i++;
        }
        for(++i; i < parent->pageHeader.numberOfKeys; i++)
        {
            parent->internalPage.entry[i - 1] = parent->internalPage.entry[i];
        }
        parent->pageHeader.numberOfKeys--;
    }
    parent_buf->is_dirty = 1;
    parent_buf->is_pinned = 0;
    if(parent->pageHeader.numberOfKeys == 0 && parent->pageHeader.parentPageNumber == 0)
    {
        buffer * header_buf = buf_read(table_id,0);
        page_t *headerPage = header_buf->frame;
        headerPage->headerPage.rootPageNumber = parent->pageHeader.rightSiblingPageNumberOrOneMorePageNumber;
        header_buf->is_dirty = 1;
        header_buf->is_pinned = 0;
        free_buf(buf_read(table_id,page->pageHeader.parentPageNumber));
        free_buf(buf);
        buffer * root_buf = buf_read(table_id,headerPage->headerPage.rootPageNumber);
        page_t *root =root_buf->frame;
        root->pageHeader.parentPageNumber = 0;
        root_buf->is_pinned = 0;
        root_buf->is_dirty = 1;
        parent_buf->is_dirty = 1;
        parent_buf->is_pinned = 0;
        return 0;
    }
    free_buf(buf);
    if(parent->pageHeader.numberOfKeys == -1)
        return delayed_merge(table_id,parent_buf);
    return 0;
}
int get_neighbor_index(int table_id,buffer * buf)
{
    int i;
    buffer * parent_buf;
    page_t * page = buf->frame;
    parent_buf = buf_read(table_id,page->pageHeader.parentPageNumber);
    page_t * parent = parent_buf->frame;
    if(parent->pageHeader.rightSiblingPageNumberOrOneMorePageNumber == buf->page_num)
    {
        parent_buf->is_pinned = 0;
        return - 1;
    }
    for(i = 0; i <= parent->pageHeader.numberOfKeys; i++)
    {
        if(parent->internalPage.entry[i].pageNumber == buf->page_num)
        {
            parent_buf->is_pinned = 0;
            return i;
        }
    }
}

void init_table(int n)
{
    db_table = malloc(sizeof(table));
    db_table->num_of_tables = 0;
    db_table->next_table_id = 1;
}

int init_db(int buf_num)
{
    buf_manager = malloc(sizeof(buffer_manager));
    if(buf_manager == NULL) return -1;
    buf_manager->capacity = buf_num;
    buf_manager->head = NULL;
    buf_manager->tail = NULL;
    buf_manager->num_of_buffers = 0;

    return 0;
}
int shutdown_db()
{   
    buffer * write_buffer;
    buffer * buf = buf_manager->head;
    if(buf == NULL) return -1;
    while(buf != NULL)
    {
        write_buffer = buf;
        buf = buf->next_of_LRU;
        flush(write_buffer);
    }
    free(buf_manager);
    for(int i = 0; i < db_table->num_of_tables; i++)
    {
        if(db_table->tableID[i].file_descriptor != 0)
        {
            close(db_table->tableID[i].file_descriptor);
        }
        db_table->tableID[i].file_descriptor = 0;
    }
    return 0;
}

buffer * buf_read(int table_id,pagenum_t pagenum)
{
    buffer * temp_buf;
    buffer * prev_buf;
    buffer * next_buf;
    temp_buf = buf_manager->head;
    while(temp_buf != NULL)
    {
        if(temp_buf->table_id == table_id && temp_buf->page_num == pagenum)
        {
            if(buf_manager->tail == temp_buf) 
            {
                if(temp_buf->is_pinned == 1) return NULL;
                temp_buf->is_pinned = 1;
                return temp_buf;
            }
            else if(buf_manager->head == temp_buf)
            {
                if(temp_buf->is_pinned == 1) return NULL;
                next_buf = temp_buf->next_of_LRU;
                buf_manager->tail->next_of_LRU = temp_buf;
                temp_buf->prev_of_LRU = buf_manager->tail;
                buf_manager->tail = temp_buf;
                buf_manager->tail->next_of_LRU = NULL;
                buf_manager->head = next_buf;
                buf_manager->head->prev_of_LRU = NULL;
                temp_buf->is_pinned = 1;
                return temp_buf;
            }
            else
            {
                if(temp_buf->is_pinned == 1) return NULL;
                temp_buf->is_pinned = 1;
                prev_buf = temp_buf->prev_of_LRU;
                next_buf = temp_buf->next_of_LRU;
                prev_buf->next_of_LRU = next_buf;
                next_buf->prev_of_LRU = prev_buf;
                buf_manager->tail->next_of_LRU = temp_buf;
                temp_buf->prev_of_LRU = buf_manager->tail;
                buf_manager->tail = temp_buf;
                buf_manager->tail->next_of_LRU = NULL;
                return temp_buf;
            }
            
        }
        temp_buf = temp_buf->next_of_LRU;
    }
    if(buf_manager->num_of_buffers == buf_manager->capacity)
    {
        temp_buf = buf_manager->head;
        while(temp_buf != NULL)
        {
            if(temp_buf->is_dirty == 1)
            {
                if(temp_buf->is_pinned == 1)break;
                buffer * write_buffer = temp_buf;
                if(buf_manager->tail == write_buffer) 
                {
                    buf_manager->tail = write_buffer->prev_of_LRU;
                    buf_manager->tail->next_of_LRU = NULL;
                    flush(write_buffer);
                    break;
                }
                else if(buf_manager->head == write_buffer)
                {
                    buf_manager->head = write_buffer->next_of_LRU;
                    buf_manager->head->prev_of_LRU = NULL;
                    flush(write_buffer);
                    break;
                }
                else
                {
                    prev_buf = write_buffer->prev_of_LRU;
                    next_buf = write_buffer->next_of_LRU;
                    prev_buf->next_of_LRU = next_buf;
                    next_buf->prev_of_LRU = prev_buf;
                    flush(write_buffer);
                    break;
                }
            }
            temp_buf = temp_buf->next_of_LRU;
        }
        if(buf_manager->num_of_buffers == buf_manager->capacity)
        {
            temp_buf = buf_manager->head;
            while(temp_buf->is_pinned == 1)
            {
                temp_buf = temp_buf->next_of_LRU;
            }
            buffer * write_buffer = temp_buf;
            if(buf_manager->tail == write_buffer) 
            {
                buf_manager->tail = write_buffer->prev_of_LRU;
                buf_manager->tail->next_of_LRU = NULL;
            } 
            else if(buf_manager->head == write_buffer)
            {
                buf_manager->head = write_buffer->next_of_LRU;
                buf_manager->head->prev_of_LRU = NULL;
            }
            else
            {
                prev_buf = write_buffer->prev_of_LRU;
                next_buf = write_buffer->next_of_LRU;
                prev_buf->next_of_LRU = next_buf;
                next_buf->prev_of_LRU = prev_buf;
            }
            flush(write_buffer);
        }
    }
    temp_buf = malloc(sizeof(buffer));
    temp_buf->frame = malloc(sizeof(page_t));
    fd = db_table->tableID[table_id-1].file_descriptor;
    file_read_page(pagenum,temp_buf->frame);
    temp_buf->is_dirty = 0;
    temp_buf->table_id = table_id;
    buf_manager->num_of_buffers++;
    temp_buf->is_pinned = 1;
    temp_buf->page_num = pagenum;
    if(buf_manager->head == NULL)
    {
            buf_manager->head = temp_buf;
            buf_manager->tail = temp_buf;
            buf_manager->head->next_of_LRU = NULL;
            buf_manager->head->prev_of_LRU = NULL;
            return temp_buf;
    }
    buf_manager->tail->next_of_LRU = temp_buf;
    temp_buf->prev_of_LRU = buf_manager->tail;
    temp_buf->next_of_LRU = NULL;
    buf_manager->tail = temp_buf;

    return temp_buf;
}
int close_table(int table_id)
{
    buffer * write_buffer, *prev_buf;
    buffer * temp_buf = buf_manager->head;
    fd = db_table->tableID[table_id-1].file_descriptor;
    if(fd == 0) return -1;
    while(temp_buf != NULL)
    {
        if(temp_buf->table_id == table_id)break;
        temp_buf = temp_buf->next_of_LRU;
    }
    if(temp_buf == buf_manager->head)
    {
        if(temp_buf->next_of_LRU == NULL)
        {
            write_buffer = temp_buf;
            flush(write_buffer);
            buf_manager->head = NULL;
            return 0;
        }
        while(temp_buf != NULL && temp_buf->table_id == table_id)
        {
            write_buffer = temp_buf;
            temp_buf = temp_buf->next_of_LRU;
            flush(write_buffer);
        }
        buf_manager->head = temp_buf;
        close(fd);
        db_table->tableID[table_id-1].file_descriptor = 0;
        return 0;
    }
    prev_buf = temp_buf->prev_of_LRU;
    while(temp_buf != NULL && temp_buf->table_id == table_id)
    {
        write_buffer = temp_buf;
        temp_buf = temp_buf->next_of_LRU;
        flush(write_buffer);
    }
    if(temp_buf == NULL)
    {
        prev_buf->next_of_LRU = NULL;
        buf_manager->tail = prev_buf;
        close(fd);
        db_table->tableID[table_id-1].file_descriptor = 0;
        return 0;
    }
    else
    {
        prev_buf->next_of_LRU = temp_buf;
        temp_buf->prev_of_LRU = prev_buf;
        close(fd);
        db_table->tableID[table_id-1].file_descriptor = 0;
        return 0;
    }
}

int flush(buffer * buf)
{
    if(buf == NULL) return -1;
    fd = db_table->tableID[buf->table_id-1].file_descriptor;
    file_write_page(buf->page_num,buf->frame);
    free(buf->frame);
    free(buf);
    buf_manager->num_of_buffers--;
    return 0;
}

void free_buf(buffer * buf)
{
    buffer * next_buf, *prev_buf;
    buffer * write_buffer = buf;
    // if(buf_manager->tail == write_buffer) 
    // {
    //     buf_manager->tail = write_buffer->prev_of_LRU;
    //     buf_manager->tail->next_of_LRU = NULL;
    // }
    // else if(buf_manager->head == write_buffer)
    // {
    //     buf_manager->head = write_buffer->next_of_LRU;
    //     if(buf_manager->head != NULL)
    //         buf_manager->head->prev_of_LRU = NULL;
    // }
    // else
    // {
    //     prev_buf = write_buffer->prev_of_LRU;
    //     next_buf = write_buffer->next_of_LRU;
    //     prev_buf->next_of_LRU = next_buf;
    //     next_buf->prev_of_LRU = prev_buf;
    // }
    buffer * header_buf = buf_read(buf->table_id,0);
    buf->frame->freePage.nextFreePageNumber = header_buf->frame->headerPage.freePageNumber;
    header_buf->frame->headerPage.freePageNumber = buf->page_num;
    //fd = db_table->tableID[buf->table_id-1].file_descriptor;
    //flush(buf);
    buf->is_dirty = 1;
    buf->is_pinned = 0;
    header_buf->is_pinned = 0;
    header_buf->is_dirty = 1;
}
