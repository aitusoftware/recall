[![Build Status](https://travis-ci.org/aitusoftware/recall.svg)](https://travis-ci.org/aitusoftware/recall)

[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/aitusoftware/recall.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/aitusoftware/recall/context:java)

![Recall Icon](https://github.com/aitusoftware/recall/raw/master/resources/img/RecallIcon.png)

Recall is an off-heap, allocation-free object store for the JVM.

## Usage

Recall is designed for use in allocation-free or low-garbage systems. Objects are expected to be
mutable in order to reduce allocation costs. For this reason, domain object should have
mutator methods for any fields that need to be serialised.

Recall can use either a standard JDK `ByteBuffer` or an
[Agrona](https://github.com/real-logic/Agrona) `UnsafeBuffer` for storage of
objects outside of the Java heap.

To use the Recall object store, implement the `Encoder`, `Decoder`, and `IdAccessor` interface for
a given object and buffer type:

```java
public class Order {
  private long id;
  private double quantity;
  private double price;

  // constructor omitted
  // getters and setters omitted
}
```

### `Encoder`

```java
public class OrderEncoder implements Encoder<ByteBuffer, Order> {
  public void store(ByteBuffer buffer, int offset, Order order) {
    buffer.putLong(offset, order.getId());
    buffer.putDouble(offset + Long.BYTES, order.getQuantity());
    buffer.putDouble(offset + Long.BYTES + Double.BYTES, order.getPrice());
  }
}
```

### `Decoder`

```java
public class OrderEncoder implements Encoder<ByteBuffer, Order> {
  public void store(ByteBuffer buffer, int offset, Order target) {
    target.setId(buffer.getLong(offset));
    target.setQuantity(buffer.getDouble(offset + Long.BYTES));
    target.setPrice(buffer.getDouble(offset + Long.BYTES + Double.BYTES));
  }
}
```

### `IdAccessor`

```java
public class OrderIdAccessor implements IdAccessor<Order> {
  public long getId(Order order) {
    return order.getId();
  }
}
```

### Store

Create a `Store`:

```java
BufferStore<ByteBuffer> store =
  new BufferStore<>(24, 100, ByteBuffer::allocateDirect, new ByteBufferOps());
```

Optionally wrap it in a `SingleTypeStore` (if only one type is going to be stored):

```java
SingleTypeStore<ByteBuffer, Order> typeStore =
  new SingleTypeStore<>(store, new OrderDecoder(), new OrderEncoder(),
    new OrderIdAccessor());
```

### Store and Load

Domain objects can be serialised to off-heap storage, and retrieved at a later time:

```java
long orderId = 42L;
Order testOrder = new Order(orderId, 12.34D, 56.78D);
typeStore.store(testOrder);

Order container = new Order(-1, -1, -1);
assert typeStore.load(orderId, container);
assert container.getQuantity() == 12.34D;
```