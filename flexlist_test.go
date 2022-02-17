package flexqueue_test

import (
	"testing"

	"github.com/gregtzar/flexqueue"
)

type Item struct {
	ID string
}

func TestFlexListPushBackPullFront(t *testing.T) {

	items := []Item{
		Item{
			ID: "A",
		},
		Item{
			ID: "B",
		},
		Item{
			ID: "C",
		},
	}

	list := flexqueue.NewFlexList()

	// perform and validate the insertions
	for i := range items {
		if ok := list.PushBack(items[i].ID, &items[i]); !ok {
			t.Errorf("expected successful push but got failed")
		}
		if !list.Has(items[i].ID) {
			t.Errorf("expected item %v to exist", items[i].ID)
		}
	}

	// verify list len
	if list.Len() != len(items) {
		t.Errorf("expected list len to be %v but got %v instead", len(items), list.Len())
	}

	// perform and validate the extractions
	for i := range items {
		index, item, ok := list.PullFront()
		if !ok {
			t.Errorf("expected successful pull but got failed")
		}
		if index != items[i].ID {
			t.Errorf("expected extracted index to be %v but got %v instead", items[i].ID, index)
		}
		if item.(*Item).ID != items[i].ID {
			t.Errorf("expected extracted item to have id %v but got %v instead", items[i].ID, item.(*Item).ID)
		}
	}

	// verify list len
	if list.Len() != 0 {
		t.Errorf("expected list len to be %v but got %v instead", 0, list.Len())
	}

	// attempt to extract from the empty queue
	if _, _, ok := list.PullFront(); ok {
		t.Errorf("expected failed pull but got success")
	}
}

func TestFlexListPushFrontPullBack(t *testing.T) {
	items := []Item{
		Item{
			ID: "A",
		},
		Item{
			ID: "B",
		},
		Item{
			ID: "C",
		},
	}

	list := flexqueue.NewFlexList()

	// perform and validate the insertions
	for i := range items {
		if ok := list.PushFront(items[i].ID, &items[i]); !ok {
			t.Errorf("expected successful push but got failed")
		}
		if !list.Has(items[i].ID) {
			t.Errorf("expected item %v to exist", items[i].ID)
		}
	}

	// verify list len
	if list.Len() != len(items) {
		t.Errorf("expected list len to be %v but got %v instead", len(items), list.Len())
	}

	// perform and validate the extractions
	for i := range items {
		index, item, ok := list.PullBack()
		if !ok {
			t.Errorf("expected successful pull but got failed")
		}
		if index != items[i].ID {
			t.Errorf("expected extracted index to be %v but got %v instead", items[i].ID, index)
		}
		if item.(*Item).ID != items[i].ID {
			t.Errorf("expected extracted item to have id %v but got %v instead", items[i].ID, item.(*Item).ID)
		}
	}

	// verify list len
	if list.Len() != 0 {
		t.Errorf("expected list len to be %v but got %v instead", 0, list.Len())
	}

	// attempt to extract from the empty queue
	if _, _, ok := list.PullBack(); ok {
		t.Errorf("expected failed pull but got success")
	}
}

func TestFlexListPushBackPullBack(t *testing.T) {

	items := []Item{
		Item{
			ID: "A",
		},
		Item{
			ID: "B",
		},
		Item{
			ID: "C",
		},
	}

	list := flexqueue.NewFlexList()

	// perform and validate the insertions
	for i := range items {
		if ok := list.PushBack(items[i].ID, &items[i]); !ok {
			t.Errorf("expected successful push but got failed")
		}
		if !list.Has(items[i].ID) {
			t.Errorf("expected item %v to exist", items[i].ID)
		}
	}

	// verify list len
	if list.Len() != len(items) {
		t.Errorf("expected list len to be %v but got %v instead", len(items), list.Len())
	}

	// perform and validate the extractions (reverse loop)
	for i := len(items) - 1; i >= 0; i-- {
		index, item, ok := list.PullBack()
		if !ok {
			t.Errorf("expected successful pull but got failed")
		}
		if index != items[i].ID {
			t.Errorf("expected extracted index to be %v but got %v instead", items[i].ID, index)
		}
		if item.(*Item).ID != items[i].ID {
			t.Errorf("expected extracted item to have id %v but got %v instead", items[i].ID, item.(*Item).ID)
		}
	}

	// verify list len
	if list.Len() != 0 {
		t.Errorf("expected list len to be %v but got %v instead", 0, list.Len())
	}

	// attempt to extract from the empty queue
	if _, _, ok := list.PullBack(); ok {
		t.Errorf("expected failed pull but got success")
	}
}

func TestFlexListPushFrontPullFront(t *testing.T) {

	items := []Item{
		Item{
			ID: "A",
		},
		Item{
			ID: "B",
		},
		Item{
			ID: "C",
		},
	}

	list := flexqueue.NewFlexList()

	// perform and validate the insertions
	for i := range items {
		if ok := list.PushFront(items[i].ID, &items[i]); !ok {
			t.Errorf("expected successful push but got failed")
		}
		if !list.Has(items[i].ID) {
			t.Errorf("expected item %v to exist", items[i].ID)
		}
	}

	// verify list len
	if list.Len() != len(items) {
		t.Errorf("expected list len to be %v but got %v instead", len(items), list.Len())
	}

	// perform and validate the extractions (reverse loop)
	for i := len(items) - 1; i >= 0; i-- {
		index, item, ok := list.PullFront()
		if !ok {
			t.Errorf("expected successful pull but got failed")
		}
		if index != items[i].ID {
			t.Errorf("expected extracted index to be %v but got %v instead", items[i].ID, index)
		}
		if item.(*Item).ID != items[i].ID {
			t.Errorf("expected extracted item to have id %v but got %v instead", items[i].ID, item.(*Item).ID)
		}
	}

	// verify list len
	if list.Len() != 0 {
		t.Errorf("expected list len to be %v but got %v instead", 0, list.Len())
	}

	// attempt to extract from the empty queue
	if _, _, ok := list.PullFront(); ok {
		t.Errorf("expected failed pull but got success")
	}
}

func TestFlexListPull(t *testing.T) {

	items := []Item{
		Item{
			ID: "A",
		},
		Item{
			ID: "B",
		},
		Item{
			ID: "C",
		},
	}

	list := flexqueue.NewFlexList()

	// perform and validate the insertions
	for i := range items {
		if ok := list.PushBack(items[i].ID, &items[i]); !ok {
			t.Errorf("expected successful push but got failed")
		}
		if !list.Has(items[i].ID) {
			t.Errorf("expected item %v to exist", items[i].ID)
		}
	}

	// perform and validate the extraction
	item, ok := list.Pull(items[1].ID)
	if !ok {
		t.Errorf("expected successful pull but got failed")
	}
	if item.(*Item).ID != items[1].ID {
		t.Errorf("expected extracted item to have id %v but got %v instead", items[1].ID, item.(*Item).ID)
	}

	// verify list len
	if list.Len() != 2 {
		t.Errorf("expected list len to be %v but got %v instead", 2, list.Len())
	}
}

func TestFlexListUpdate(t *testing.T) {

	items := []Item{
		Item{
			ID: "A",
		},
		Item{
			ID: "B",
		},
		Item{
			ID: "C",
		},
	}

	updateItem := Item{
		ID: "D",
	}

	list := flexqueue.NewFlexList()

	// perform and validate the insertions
	for i := range items {
		if ok := list.PushBack(items[i].ID, &items[i]); !ok {
			t.Errorf("expected successful push but got failed")
		}
		if !list.Has(items[i].ID) {
			t.Errorf("expected item %v to exist", items[i].ID)
		}
	}

	// verify that an update is rejected if index does not exist
	if ok := list.Update("foo", &items[1]); ok {
		t.Errorf("expected failed push but got success")
	}

	// verify that an update succeeds for an existing index
	if ok := list.Update(items[1].ID, &updateItem); !ok {
		t.Errorf("expected successful push but got failed")
	}

	// confirm the update
	item, ok := list.Read(items[1].ID)
	if !ok {
		t.Errorf("expected successful read but got failed")
	}
	if item.(*Item).ID != updateItem.ID {
		t.Errorf("expected updated item to have id %v but got %v instead", updateItem.ID, item.(*Item).ID)
	}

	// verify list len did not change
	if list.Len() != len(items) {
		t.Errorf("expected list len to be %v but got %v instead", len(items), list.Len())
	}
}

func TestFlexListDuplicateIndex(t *testing.T) {

	item := Item{
		ID: "A",
	}

	list := flexqueue.NewFlexList()

	if ok := list.PushBack(item.ID, &item); !ok {
		t.Errorf("expected successful push but got failed")
	}
	if !list.Has(item.ID) {
		t.Errorf("expected item %v to exist", item.ID)
	}

	// verify that a duplicated item is rejected based on index
	if ok := list.PushBack(item.ID, &item); ok {
		t.Errorf("expected failed push but got success")
	}

	// verify list len did not change
	if list.Len() != 1 {
		t.Errorf("expected list len to be %v but got %v instead", 1, list.Len())
	}
}

func TestFlexListRead(t *testing.T) {

	items := []Item{
		Item{
			ID: "A",
		},
		Item{
			ID: "B",
		},
		Item{
			ID: "C",
		},
	}

	list := flexqueue.NewFlexList()

	// perform the insertions
	for i := range items {
		if ok := list.PushBack(items[i].ID, &items[i]); !ok {
			t.Errorf("expected successful push but got failed")
		}
	}

	// verify read from front
	index, item, ok := list.ReadFront()
	if !ok {
		t.Errorf("expected successful read but got failed")
	}
	if index != items[0].ID {
		t.Errorf("expected extracted index to be %v but got %v instead", items[0].ID, index)
	}
	if item.(*Item).ID != items[0].ID {
		t.Errorf("expected extracted item to have id %v but got %v instead", items[0].ID, item.(*Item).ID)
	}

	// verify read from back
	index, item, ok = list.ReadBack()
	if !ok {
		t.Errorf("expected successful read but got failed")
	}
	if index != items[2].ID {
		t.Errorf("expected extracted index to be %v but got %v instead", items[2].ID, index)
	}
	if item.(*Item).ID != items[2].ID {
		t.Errorf("expected extracted item to have id %v but got %v instead", items[2].ID, item.(*Item).ID)
	}

	// verify read from index
	item, ok = list.Read(items[1].ID)
	if !ok {
		t.Errorf("expected successful read but got failed")
	}
	if item.(*Item).ID != items[1].ID {
		t.Errorf("expected extractetd item to have id %v but got %v instead", items[1].ID, item.(*Item).ID)
	}

	// verify list len did not change
	if list.Len() != len(items) {
		t.Errorf("expected list len to be %v but got %v instead", len(items), list.Len())
	}
}

func TestFlexListHas(t *testing.T) {

	item := Item{
		ID: "A",
	}

	list := flexqueue.NewFlexList()

	if list.Has(item.ID) {
		t.Errorf("expected item %v to not exist", item.ID)
	}

	if ok := list.PushBack(item.ID, &item); !ok {
		t.Errorf("expected successful push but got failed")
	}

	if !list.Has(item.ID) {
		t.Errorf("expected item %v to exist", item.ID)
	}
}

func TestFlexListLen(t *testing.T) {

	item := Item{
		ID: "A",
	}

	list := flexqueue.NewFlexList()

	if list.Len() != 0 {
		t.Errorf("expected list len to be 0 but got %v instead", list.Len())
	}

	if ok := list.PushBack(item.ID, &item); !ok {
		t.Errorf("expected successful push but got failed")
	}

	if list.Len() != 1 {
		t.Errorf("expected list len to be 1 but got %v instead", list.Len())
	}
}

func TestFlexListRemove(t *testing.T) {

	item := Item{
		ID: "A",
	}

	list := flexqueue.NewFlexList()

	if ok := list.PushBack(item.ID, &item); !ok {
		t.Errorf("expected successful push but got failed")
	}

	if !list.Has(item.ID) {
		t.Errorf("expected item %v to exist", item.ID)
	}

	if ok := list.Remove(item.ID); !ok {
		t.Errorf("expected successful remove but got failed")
	}

	if list.Has(item.ID) {
		t.Errorf("expected item %v to not exist", item.ID)
	}

	if list.Len() != 0 {
		t.Errorf("expected list len to be 0 but got %v instead", list.Len())
	}
}
