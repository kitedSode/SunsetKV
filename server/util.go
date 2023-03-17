package main

/**
关于日志的位置:
1.我们在不同的server之间或者是server与状态机之间交互时，我们的日志index都是以状态机的计数（从1开始）为准。
1. 在server中存储的log[]是从0开始计算index的，而在状态机中的存储的index是从1开始计算的所以会比实际日志中的位置多1，
所以在查找实际的日志index时要进行 - 1 操作，在提交给状态机时根据存储的index要再加上1。
2. 由于快照技术会舍弃一部分日志，所以计算日志的实际位置时要减去快照最后的index来获得准确的server中该log的index。
同理在可以通过server中log的index + 1 + rf.lastIncludedIndex 来确定最后一条log在状态机中的位置

*/

// 返回的日志index为实际存储的index + 1 + rf.lastIncludedIndex
func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) + rf.lastIncludedIndex
}

// 将状态机的index转换为该server的log[]中的真实位置
func (rf *Raft) getServerLogIndex(machineLogIndex int) int {
	return machineLogIndex - rf.lastIncludedIndex - 1
}

func (rf *Raft) getLogByIndex(index int) LogEntry {
	//fmt.Println("len_logs:", len(rf.logs))
	//if index - rf.lastIncludedIndex - 1 == 0{
	//	fmt.Printf("index: %d, lastIncludedIndex: %d, len(logs): %d\n", index, rf.lastIncludedIndex, len(rf.logs))
	//}
	return rf.logs[index-rf.lastIncludedIndex-1]
}

// if index > rf.lastIncludedIndex: return true log's index
// if index == rf.lastIncludedIndex: return rf.lastIncludedTerm
// if index < rf.lastIncludedIndex: return -1
func (rf *Raft) getLogTermByIndex(index int) int {
	if index > rf.lastIncludedIndex {
		return rf.logs[index-rf.lastIncludedIndex-1].Term
	} else if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}

	return -1
}
