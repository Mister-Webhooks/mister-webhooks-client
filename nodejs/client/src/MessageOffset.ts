export class MessageOffset {
  readonly value: bigint

  constructor(value: bigint) {
    this.value = value
  }

  static fromString(value: string) {
    return new MessageOffset(BigInt(value))
  }

  toJSON() {
    return this.value.toString()
  }

  toString() {
    return this.value.toString()
  }

  toLocaleString() {
    return this.value.toLocaleString()
  }

  valueOf() {
    return this.value.valueOf()
  }
}
