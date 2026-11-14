// A Min-Heap is a binary tree where:
// - The parent always has a priority smaller than or equal to its children.
// - The root always contains the smallest value.
// - In our case, the smallest value is the earliest nextRetryAt.

// Stores the failed message until they are ready for another attemp
export class PriorityQueue<T> {
  private heap: T[] = [];

  constructor(private readonly compareFn: (a: T, b: T) => number) {}

  push(value: T): void {
    // Add the heap value at the end of the array
    this.heap.push(value);

    // Restore the Min-Heap order starting from the last index
    this.bubbleUp();
  }

  // when we want to add an element we add it at the end then we bubble it to its correct position
  // smalles element are the parent and children are the largest this makes it useful to carry smaller elements taks faster
  private bubbleUp(): void {
    // Move the newly inserted value towards the root
    let index = this.heap.length - 1;

    while (index > 0) {
      const parentIndex = Math.floor((index - 1) / 2);

      // If current item is not smaller than its parent then it is satisfied
      if (this.compareFn(this.heap[index], this.heap[parentIndex]) >= 0) {
        break;
      }

      // Swap child and parent
      [[this.heap[index]], [this.heap[parentIndex]]] = [
        [this.heap[parentIndex]],
        [this.heap[index]],
      ];

      // Move pointer up to the parent's position and continue checking
      index = parentIndex;
    }
  }

  // when we want to remove a element we cant remove directly we just say pop it and bubble it down to last and then remove it
  private bubbleDown(): void {
    let index = 0;

    while (true) {
      const leftIdx = 2 * index + 1;
      const rightIdx = 2 * index + 2;

      let smallestIdx = index;

      if (
        leftIdx < this.heap.length &&
        this.compareFn(this.heap[leftIdx], this.heap[smallestIdx]) < 0
      ) {
        smallestIdx = leftIdx;
      }

      if (
        rightIdx < this.heap.length &&
        this.compareFn(this.heap[rightIdx], this.heap[smallestIdx]) < 0
      ) {
        smallestIdx = rightIdx;
      }

      if (smallestIdx === index) break;

      [this.heap[index], this.heap[smallestIdx]] = [
        this.heap[smallestIdx],
        this.heap[index],
      ];

      index = smallestIdx;
    }
  }

  peek(): T | undefined {
    return this.heap[0];
  }

  pop(): T | undefined {
    // Handle an empty heap
    if (this.heap.length === 0) {
      return undefined;
    }

    // Save the root because it has the highest priority
    const root = this.heap[0];

    // Remove the last element
    const last = this.heap.pop()!;

    // If elements remain, move the last element to the root
    if (this.heap.length > 0) {
      this.heap[0] = last;

      // Restore Min-Heap order
      this.bubbleDown();
    }

    return root;
  }

  isEmpty(): boolean {
    return this.heap.length === 0;
  }
}
