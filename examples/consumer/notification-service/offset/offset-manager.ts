// Tracks the next offset to fetch
export class OffsetManager {
  private currentOffset = 0;

  getOffset(): number {
    return this.currentOffset;
  }

  advance(): void {
    this.currentOffset++;
  }
}
