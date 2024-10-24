package node

// Node represents the physical aspect of the Worker.
type Node struct {
	Name            string
	Ip              string
	Cores           int
	Memory          int
	MemoryAllocated int
	Disk            int
	DiskAllocated   int
	Role            string
	TaskCount       int
}

func NewNode(worker string, api string, role string) *Node {
	return &Node{
		Name: worker,
		Ip:   api,
		Role: role,
	}
}
