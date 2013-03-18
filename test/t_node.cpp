#include <gtest/gtest.h>

#define private public
#define protected public

#include "tree/node.h"
#include "tree/tree.h"
#include "helper.h"

using namespace cascadb;
using namespace std;

TEST(Pivot, serialize)
{
    char buffer[4096];
    Block blk(buffer, 0, 4096);
    BlockReader reader(&blk);
    BlockWriter writer(&blk);
    
    LexicalComparator comp;
    MsgBuf mb1(&comp);
    PUT(mb1, "a", "1");
    PUT(mb1, "b", "1");
    PUT(mb1, "c", "1");
    
    Pivot pv1("abc", 5U, &mb1);
    pv1.write_to(writer);
    
    EXPECT_EQ(pv1.size(), blk.size());
    
    Pivot pv2;
    MsgBuf mb2(&comp);
    pv2.msgbuf = &mb2;
    pv2.read_from(reader);
    
    EXPECT_EQ("abc", pv2.key);
    EXPECT_EQ(5U, pv2.child);
    EXPECT_EQ(3U, mb2.count());
    CHK_MSG(mb2.get(0), Put, "a", "1");
    CHK_MSG(mb2.get(1), Put, "b", "1");
    CHK_MSG(mb2.get(2), Put, "c", "1");
}

TEST(Record, serialize)
{
    char buffer[4096];
    Block blk(buffer, 0, 4096);
    BlockReader reader(&blk);
    BlockWriter writer(&blk);
    
    Record rec1("a", "1");
    rec1.write_to(writer);
    
    EXPECT_EQ(rec1.size(), blk.size());
    
    Record rec2;
    rec2.read_from(reader);
    EXPECT_EQ("a", rec2.key);
    EXPECT_EQ("1", rec2.value);
}

TEST(Tree, bootstrap)
{
    LexicalComparator comp;
    TestNodeStore store;

    Options opts;
    opts.comparator = &comp;
    opts.inner_node_msg_count = 4;
    opts.inner_node_children_number = 2;
    opts.leaf_node_record_count = 4;

    Tree *tree = new Tree("", opts, &store);
    EXPECT_TRUE(tree->init());
    EXPECT_TRUE(tree->root_!=NULL);
    
    InnerNode *n1 = tree->root_;
    EXPECT_EQ(NID_START, n1->nid_);
    
    // the first msgbuf is created
    n1->put("a", "1");
    n1->put("b", "1");
    n1->put("c", "1");
    EXPECT_EQ(NID_NIL, n1->first_child_);
    EXPECT_EQ(3U, n1->first_msgbuf_->count());
    n1->put("d", "1");
    // limit of the first msg reached
    // create leaf#1
    EXPECT_EQ(0U, n1->first_msgbuf_->count());
    EXPECT_TRUE(n1->first_child_ != NID_NIL);
    LeafNode *l1 = (LeafNode*)store.get_raw(n1->first_child_);
    EXPECT_EQ(NID_LEAF_START, l1->nid_);
    EXPECT_EQ(4U, l1->records_.size());
    CHK_REC(l1->records_[0], "a", "1");
    CHK_REC(l1->records_[1], "b", "1");
    CHK_REC(l1->records_[2], "c", "1");
    CHK_REC(l1->records_[3], "d", "1");
    
    // go on filling leaf#1's msgbuf
    n1->put("e", "1");
    n1->put("f", "1");
    n1->put("g", "1");
    EXPECT_EQ(3U, n1->first_msgbuf_->count());
    
    n1->put("h", "1");
    // cascading into #leaf1, and split into two leafs
    EXPECT_EQ(0U, n1->first_msgbuf_->count());
    EXPECT_EQ(l1->nid_, n1->first_child_);
    EXPECT_EQ(1U, n1->pivots_.size());
    EXPECT_TRUE(n1->pivots_[0].key == "e");
    EXPECT_TRUE(n1->pivots_[0].child != NID_NIL);
    EXPECT_TRUE(n1->pivots_[0].msgbuf != NULL);
    LeafNode *l2 = (LeafNode*)store.get_raw(n1->pivots_[0].child);
    EXPECT_EQ(NID_LEAF_START+1, l2->nid_);
    EXPECT_EQ(4U, l1->records_.size());
    CHK_REC(l1->records_[0], "a", "1");
    CHK_REC(l1->records_[1], "b", "1");
    CHK_REC(l1->records_[2], "c", "1");
    CHK_REC(l1->records_[3], "d", "1");
    EXPECT_EQ(4U, l2->records_.size());
    CHK_REC(l2->records_[0], "e", "1");
    CHK_REC(l2->records_[1], "f", "1");
    CHK_REC(l2->records_[2], "g", "1");
    CHK_REC(l2->records_[3], "h", "1");
    
    n1->put("a", "2");
    n1->put("b", "2");
    n1->put("bb", "1");
    EXPECT_EQ(70U, n1->size());

    n1->put("e", "2");
    
    // cascade into #leaf1 and force leaf#1 split,
    // then propogate to node#1 and generate new root
    
    // node#3(the new root)
    EXPECT_NE(tree->root_, n1);
    InnerNode *n3 = tree->root_;
    EXPECT_EQ(NID_START+2, n3->nid_);
    EXPECT_EQ(n3->first_child_, n1->nid_);
    EXPECT_EQ(n3->first_msgbuf_->count(), 0U);
    
    // node#1
    EXPECT_EQ(0U, n1->first_msgbuf_->count());
    EXPECT_EQ(l1->nid_, n1->first_child_);
    EXPECT_EQ(1U, n1->pivots_.size());
    EXPECT_TRUE(n1->pivots_[0].key == "bb");
    EXPECT_TRUE(n1->pivots_[0].child != NID_NIL);
    EXPECT_TRUE(n1->pivots_[0].child != l2->nid_);
    EXPECT_TRUE(n1->pivots_[0].msgbuf != NULL);
    LeafNode *l3 = (LeafNode*)store.get_raw(n1->pivots_[0].child);
    EXPECT_EQ(NID_LEAF_START+2, l3->nid_);
    EXPECT_EQ(2U, l1->records_.size());
    CHK_REC(l1->records_[0], "a", "2");
    CHK_REC(l1->records_[1], "b", "2");
    EXPECT_EQ(3U, l3->records_.size());
    CHK_REC(l3->records_[0], "bb", "1");
    CHK_REC(l3->records_[1], "c", "1");
    CHK_REC(l3->records_[2], "d", "1");
    EXPECT_EQ(37U, n1->size());
    
    // node#2
    EXPECT_EQ(n3->pivots_.size(), 1U);
    EXPECT_TRUE(n3->pivots_[0].key == "e");
    EXPECT_TRUE(n3->pivots_[0].child != NID_NIL);
    InnerNode *n2 = (InnerNode*)store.get_raw(n3->pivots_[0].child);
    EXPECT_EQ(NID_START+1, n2->nid_);
    EXPECT_EQ(1U, n2->first_msgbuf_->count());
    CHK_MSG(n2->first_msgbuf_->get(0),  Put, "e", "2");
    EXPECT_EQ(l2->nid_, n2->first_child_);
    EXPECT_EQ(0U, n2->pivots_.size());
    EXPECT_EQ(29U, n2->size());
    
    n3->put("abc", "1");
    n3->put("bb", "2");
    n3->put("ee", "1");
    n3->put("f", "2");
    // cascading down, no split
    EXPECT_EQ(tree->root_, n3);
    EXPECT_EQ(NID_START+2, n3->nid_);
    EXPECT_EQ(n3->first_child_, n1->nid_);
    EXPECT_EQ(0U, n3->first_msgbuf_->count());
    EXPECT_EQ(n3->pivots_[0].child, n2->nid_);
    EXPECT_EQ(2U, n3->pivots_[0].msgbuf->count());
    CHK_MSG(n3->pivots_[0].msgbuf->get(0), Put, "ee", "1");
    CHK_MSG(n3->pivots_[0].msgbuf->get(1), Put, "f", "2");
    
    EXPECT_EQ(1U, n1->first_msgbuf_->count());
    CHK_MSG(n1->first_msgbuf_->get(0), Put, "abc", "1");
    EXPECT_EQ(1U, n1->pivots_[0].msgbuf->count());
    CHK_MSG(n1->pivots_[0].msgbuf->get(0), Put, "bb", "2");

    n3->put("abcd", "1");
    n3->put("g", "2");
    // l2 split
    EXPECT_TRUE(tree->root_ == n3);
    EXPECT_EQ(NID_START+2, n3->nid_);
    EXPECT_TRUE(n3->first_child_ == n1->nid_);
    EXPECT_EQ(1U, n3->first_msgbuf_->count());
    CHK_MSG(n3->first_msgbuf_->get(0), Put, "abcd", "1");
    EXPECT_TRUE(n3->pivots_[0].child == n2->nid_);
    EXPECT_EQ(0U, n3->pivots_[0].msgbuf->count());
    
    EXPECT_EQ(1U, n2->pivots_.size());
    EXPECT_TRUE(n2->first_child_ == l2->nid_);
    EXPECT_EQ(0U, n2->first_msgbuf_->count());
    EXPECT_TRUE(n2->pivots_[0].child != NID_NIL);
    EXPECT_EQ("f", n2->pivots_[0].key);
    EXPECT_EQ(0U, n2->pivots_[0].msgbuf->count());
    LeafNode *l4 = (LeafNode*)store.get_raw(n2->pivots_[0].child);
    EXPECT_EQ(NID_LEAF_START+3, l4->nid_);
    EXPECT_EQ(2U, l2->records_.size());
    CHK_REC(l2->records_[0], "e", "2");
    CHK_REC(l2->records_[1], "ee", "1");
    EXPECT_EQ(3U, l4->records_.size());
    CHK_REC(l4->records_[0], "f", "2");
    CHK_REC(l4->records_[1], "g", "2");
    CHK_REC(l4->records_[2], "h", "1");
    
    delete tree;
}

TEST(InnerNode, serialize)
{
    char buffer[4096];
    Block blk(buffer, 0, 4096);
    BlockReader reader(&blk);
    BlockWriter writer(&blk);

    LexicalComparator comp;
    TestNodeStore store;

    Options opts;
    opts.comparator = &comp; 
    opts.inner_node_msg_count = 4;
    opts.inner_node_children_number = 2;
    opts.leaf_node_record_count = 4;
    Tree *tree = new Tree("", opts, &store);

    InnerNode n1("", NID_START);
    n1.set_tree(tree);
    n1.bottom_ = true;
    n1.first_child_ = NID_LEAF_START;
    n1.first_msgbuf_ = new MsgBuf(&comp);
    PUT(*n1.first_msgbuf_, "a", "1");
    PUT(*n1.first_msgbuf_, "b", "1");
    PUT(*n1.first_msgbuf_, "c", "1");
    n1.pivots_.resize(1);
    n1.pivots_[0].key = Slice("d").clone();
    n1.pivots_[0].child = NID_LEAF_START + 1;
    n1.pivots_[0].msgbuf = new MsgBuf(&comp);

    EXPECT_TRUE(n1.write_to(writer) == true);

    InnerNode n2("", NID_START);
    n2.set_tree(tree);
    EXPECT_TRUE(n2.read_from(reader) == true);
    EXPECT_EQ(n2.size(), blk.size());

    EXPECT_TRUE(n2.bottom_ == true);
    EXPECT_EQ(NID_LEAF_START, n2.first_child_);
    EXPECT_TRUE(n2.first_msgbuf_ != NULL);
    EXPECT_EQ(3U, n2.first_msgbuf_->count());
    CHK_MSG(n2.first_msgbuf_->get(0), Put, "a", "1");
    CHK_MSG(n2.first_msgbuf_->get(1), Put, "b", "1");
    CHK_MSG(n2.first_msgbuf_->get(2), Put, "c", "1");
    EXPECT_EQ(1U, n2.pivots_.size());
    EXPECT_EQ("d", n2.pivots_[0].key);
    EXPECT_EQ(NID_LEAF_START+1, n2.pivots_[0].child);
    EXPECT_TRUE(n2.pivots_[0].msgbuf != NULL);
    EXPECT_EQ(0U, n2.pivots_[0].msgbuf->count());

    delete tree;
}

/*
TEST(Leaf, serialize)
{
    char buffer[4096];
    Block blk(buffer, 0, 4096);
    BlockReader reader(&blk);
    BlockWriter writer(&blk);

    LexicalComparator comp;
    TestNodeStore store;

    Options opts;
    opts.comparator = &comp; 
    Tree *tree = new Tree("", opts, &store);

    tree->max.inner_node_msg_count = 4;
    tree->max.inner_node_children_number = 2;
    tree->max.leaf_node_record_count = 4;

    LeafNode l1("", NID_LEAF_START);
    l1.set_tree(tree);
    l1.left_sibling_ = NID_LEAF_START+1;
    l1.right_sibling_ = NID_LEAF_START+2;
    l1.records_.push_back(Record("a", "1"));
    l1.records_.push_back(Record("b", "1"));
    l1.records_.push_back(Record("c", "1"));

    EXPECT_TRUE(l1.write_to(writer) == true);

    LeafNode l2("", NID_LEAF_START);
    l2.set_tree(tree);
    EXPECT_TRUE(l2.read_from(reader) == true);
    EXPECT_EQ(l2.size(), blk.size());

    EXPECT_EQ(NID_LEAF_START+1, l2.left_sibling_);
    EXPECT_EQ(NID_LEAF_START+2, l2.right_sibling_);
    EXPECT_EQ(3U, l2.records_.size());
    CHK_REC(l2.records_[0], "a", "1");
    CHK_REC(l2.records_[1], "b", "1");
    CHK_REC(l2.records_[2], "c", "1");

    delete tree;
}

TEST(InnerNode, write_msgbuf)
{
    LexicalComparator comp;
    TestNodeStore store;

    Options opts;
    opts.comparator = &comp; 
    Tree *tree = new Tree("", opts, &store);

    tree->max.inner_node_msg_count = 4;
    tree->init();

    InnerNode *n1 = tree->new_inner_node();

    n1->bottom_ = true;
    n1->first_child_ = NID_NIL;
    n1->first_msgbuf_ = new MsgBuf(&comp);
    n1->pivots_.resize(1);
    n1->pivots_[0].key = "d";
    n1->pivots_[0].child = NID_NIL;
    n1->pivots_[0].msgbuf = new MsgBuf(&comp);
    n1->msgcnt_ = 0;
    n1->msgbufsz_ = n1->first_msgbuf_->size() + n1->pivots_[0].msgbuf->size();

    n1->put("a", "1");
    EXPECT_EQ(1U, n1->first_msgbuf_->count());
    EXPECT_EQ(0U, n1->pivots_[0].msgbuf->count());

    n1->put("e", "1");
    EXPECT_EQ(1U, n1->first_msgbuf_->count());
    EXPECT_EQ(1U, n1->pivots_[0].msgbuf->count());

    n1->dec_ref();
    delete tree;
    store.close();
}

TEST(InnerNode, cascade)
{
    LexicalComparator comp;
    TestNodeStore store;

    Options opts;
    opts.comparator = &comp; 
    Tree *tree = new Tree("", opts, &store);

    tree->max.inner_node_msg_count = 4;
    tree->init();

    InnerNode *n1 = tree->new_inner_node();
    InnerNode *n2 = tree->new_inner_node();
    InnerNode *n3 = tree->new_inner_node();

    n1->bottom_ = false;
    n1->first_child_ = n2->nid_;
    n1->first_msgbuf_ = new MsgBuf(&comp);
    n1->pivots_.resize(1);
    n1->pivots_[0].key = "d";
    n1->pivots_[0].child = n3->nid_;
    n1->pivots_[0].msgbuf = new MsgBuf(&comp);
    n1->msgcnt_ = 0;
    n1->msgbufsz_ = n1->first_msgbuf_->size() + n1->pivots_[0].msgbuf->size();

    n2->bottom_ = true;
    n2->first_child_ = NID_NIL;
    n2->first_msgbuf_ = new MsgBuf(&comp);
    n2->msgcnt_ = 0;
    n2->msgbufsz_ = n2->first_msgbuf_->size();

    n3->bottom_ = true;
    n3->first_child_ = NID_NIL;
    n3->first_msgbuf_ = new MsgBuf(&comp);
    n3->msgcnt_ = 0;
    n3->msgbufsz_ = n3->first_msgbuf_->size();

    n1->put("a","1");
    n1->put("b","1");
    n1->put("c","1");
    n1->put("d","1");

    EXPECT_EQ(1U, n1->msgcnt_);
    EXPECT_EQ(3U, n2->msgcnt_);
    EXPECT_EQ(0U, n3->msgcnt_);

    n1->put("e","1");
    n1->put("f","1");
    n1->put("a","2");

    EXPECT_EQ(1U, n1->msgcnt_);
    EXPECT_EQ(3U, n2->msgcnt_);
    EXPECT_EQ(3U, n3->msgcnt_);

    n1->dec_ref();
    n2->dec_ref();
    n3->dec_ref();
    delete tree;
}
*/

TEST(InnerNode, add_pivot)
{
    LexicalComparator comp;
    TestNodeStore store;

    Options opts;
    opts.comparator = &comp;
    opts.inner_node_children_number = 4;
    
    Tree *tree = new Tree("", opts, &store);

    tree->init();

    InnerNode *n1 = tree->new_inner_node();

    n1->bottom_ = true;
    n1->first_child_ = NID_START + 100;
    n1->first_msgbuf_ = new MsgBuf(&comp);
    n1->msgcnt_ = 0;
    n1->msgbufsz_ = n1->first_msgbuf_->size();

    std::vector<DataNode*> path;

    path.push_back(n1);
    n1->inc_ref();
    n1->write_lock();
    n1->add_pivot("e", NID_START + 101, path);
    EXPECT_EQ(1U, n1->pivots_.size());
    EXPECT_EQ("e", n1->pivots_[0].key);

    path.push_back(n1);
    n1->inc_ref();
    n1->write_lock();
    n1->add_pivot("d", NID_START + 102, path);
    EXPECT_EQ(2U, n1->pivots_.size());
    EXPECT_EQ("d", n1->pivots_[0].key);
    EXPECT_EQ("e", n1->pivots_[1].key);

    path.push_back(n1);
    n1->inc_ref();
    n1->write_lock();
    n1->add_pivot("f", NID_START + 103, path);
    EXPECT_EQ(3U, n1->pivots_.size());
    EXPECT_EQ("d", n1->pivots_[0].key);
    EXPECT_EQ("e", n1->pivots_[1].key);
    EXPECT_EQ("f", n1->pivots_[2].key);

    n1->dec_ref();
    delete tree;
}

TEST(InnerNode, split)
{
    LexicalComparator comp;
    TestNodeStore store;

    Options opts;
    opts.comparator = &comp; 
    opts.inner_node_children_number = 3;

    Tree *tree = new Tree("", opts, &store);

    tree->init();

    InnerNode *n1 = tree->new_inner_node();
    InnerNode *n2 = tree->new_inner_node();

    n1->bottom_ = false;
    n1->first_child_ = n2->nid_;
    n1->first_msgbuf_ = new MsgBuf(&comp);
    n1->msgcnt_ = 0;
    n1->msgbufsz_ = n1->first_msgbuf_->size();

    n2->bottom_ = false;
    n2->first_child_ = NID_START + 100;
    n2->first_msgbuf_ = new MsgBuf(&comp);
    n2->msgcnt_ = 0;
    n2->msgbufsz_ = n2->first_msgbuf_->size();

    std::vector<DataNode*> path;

    path.push_back(n1);
    path.push_back(n2);
    n1->inc_ref();
    n1->write_lock();
    n2->inc_ref();
    n2->write_lock();
    n2->add_pivot("e", NID_START + 101, path);
    EXPECT_EQ(1U, n2->pivots_.size());
    EXPECT_EQ("e", n2->pivots_[0].key);

    path.push_back(n1);
    path.push_back(n2);
    n1->inc_ref();
    n1->write_lock();
    n2->inc_ref();
    n2->write_lock();
    n2->add_pivot("d", NID_START + 102, path);
    EXPECT_EQ(2U, n2->pivots_.size());
    EXPECT_EQ("d", n2->pivots_[0].key);
    EXPECT_EQ("e", n2->pivots_[1].key);

    path.push_back(n1);
    path.push_back(n2);
    n1->inc_ref();
    n1->write_lock();
    n2->inc_ref();
    n2->write_lock();
    n2->add_pivot("f", NID_START + 103, path);
    EXPECT_EQ(1U, n2->pivots_.size());
    EXPECT_EQ("d", n2->pivots_[0].key);
    EXPECT_EQ(1U, n1->pivots_.size());
    EXPECT_EQ("e", n1->pivots_[0].key);
    EXPECT_NE(NID_NIL, n1->pivots_[0].child);
    InnerNode *n3 = (InnerNode*)tree->load_node(n1->pivots_[0].child);
    EXPECT_TRUE(n3 != NULL);
    EXPECT_EQ(1U, n3->pivots_.size());
    EXPECT_EQ("f", n3->pivots_[0].key);

    n1->dec_ref();
    n2->dec_ref();
    n3->dec_ref();
    delete tree;
}

TEST(InnerNode, split_recursive_to_root)
{
    
}

TEST(InnerNode, rm_pivot)
{
    
}

TEST(InnerNode, rm_pivot_recursive)
{
    
}

TEST(InnerNode, rm_pivot_recursive_to_root)
{
    
}

TEST(InnerNode, find)
{
    
}

/*
TEST(LeafNode, cascade)
{
    LexicalComparator comp;
    TestNodeStore store;

    Options opts;
    opts.comparator = &comp; 
    Tree *tree = new Tree("", opts, &store);

    tree->max.leaf_node_record_count = 100;
    tree->init();

    InnerNode *n1 = tree->new_inner_node();
    LeafNode *l1 = tree->new_leaf_node();

    n1->bottom_ = true;
    n1->first_child_ = l1->nid_;
    n1->first_msgbuf_ = new MsgBuf(&comp);
    PUT(*n1->first_msgbuf_, "a", "1");
    PUT(*n1->first_msgbuf_, "b", "1");
    PUT(*n1->first_msgbuf_, "c", "1");
    PUT(*n1->first_msgbuf_, "d", "1");
    n1->msgcnt_ = 4;
    n1->msgbufsz_ = n1->first_msgbuf_->size();

    n1->read_lock();
    l1->cascade(n1->first_msgbuf_, n1);
    EXPECT_EQ(0U, n1->first_msgbuf_->count());
    EXPECT_EQ(4U, l1->records_.size());

    PUT(*n1->first_msgbuf_, "e", "1");
    PUT(*n1->first_msgbuf_, "f", "1");
    PUT(*n1->first_msgbuf_, "g", "1");
    PUT(*n1->first_msgbuf_, "h", "1");
    n1->msgcnt_ = 4;
    n1->msgbufsz_ = n1->first_msgbuf_->size();

    n1->read_lock();
    l1->cascade(n1->first_msgbuf_, n1);
    EXPECT_EQ(0U, n1->first_msgbuf_->count());
    EXPECT_EQ(8U, l1->records_.size());

    PUT(*n1->first_msgbuf_, "a", "2");
    PUT(*n1->first_msgbuf_, "c", "2");
    PUT(*n1->first_msgbuf_, "f", "2");
    PUT(*n1->first_msgbuf_, "h", "2");
    n1->msgcnt_ = 4;
    n1->msgbufsz_ = n1->first_msgbuf_->size();

    n1->read_lock();
    l1->cascade(n1->first_msgbuf_, n1);
    EXPECT_EQ(0U, n1->first_msgbuf_->count());
    EXPECT_EQ(8U, l1->records_.size());
    CHK_REC(l1->records_[0], "a", "2");
    CHK_REC(l1->records_[1], "b", "1");
    CHK_REC(l1->records_[2], "c", "2");
    CHK_REC(l1->records_[3], "d", "1");
    CHK_REC(l1->records_[4], "e", "1");
    CHK_REC(l1->records_[5], "f", "2");
    CHK_REC(l1->records_[6], "g", "1");
    CHK_REC(l1->records_[7], "h", "2");

    n1->dec_ref();
    l1->dec_ref();
    delete tree;
}

TEST(LeafNode, split)
{
    LexicalComparator comp;
    TestNodeStore store;

    Options opts;
    opts.comparator = &comp; 
    Tree *tree = new Tree("", opts, &store);

    tree->max.leaf_node_record_count = 4;
    tree->init();

    InnerNode *n1 = tree->new_inner_node();
    LeafNode *l1 = tree->new_leaf_node();

    tree->root_->first_child_ = n1->nid_;

    n1->bottom_ = true;
    n1->first_child_ = l1->nid_;
    n1->first_msgbuf_ = new MsgBuf(&comp);
    PUT(*n1->first_msgbuf_, "a", "1");
    PUT(*n1->first_msgbuf_, "b", "1");
    PUT(*n1->first_msgbuf_, "c", "1");
    PUT(*n1->first_msgbuf_, "d", "1");
    n1->msgcnt_ = 4;
    n1->msgbufsz_ = n1->first_msgbuf_->size();

    n1->read_lock();
    l1->cascade(n1->first_msgbuf_, n1);

    PUT(*n1->first_msgbuf_, "e", "1");
    PUT(*n1->first_msgbuf_, "f", "1");
    PUT(*n1->first_msgbuf_, "g", "1");
    PUT(*n1->first_msgbuf_, "h", "1");
    n1->msgcnt_ = 4;
    n1->msgbufsz_ = n1->first_msgbuf_->size();

    n1->read_lock();
    l1->cascade(n1->first_msgbuf_, n1);

    EXPECT_EQ(1U, n1->pivots_.size());
    EXPECT_EQ("e", n1->pivots_[0].key);
    EXPECT_NE(NID_NIL, n1->pivots_[0].child);
    LeafNode *l2 = (LeafNode*)tree->load_node(n1->pivots_[0].child);
    EXPECT_TRUE(l2 != NULL);

    EXPECT_EQ(4U, l1->records_.size());
    EXPECT_EQ(4U, l2->records_.size());

    n1->dec_ref();
    l1->dec_ref();
    l2->dec_ref();
    delete tree;
}
*/


TEST(LeafNode, merge)
{
}

TEST(LeafNode, find)
{
}
