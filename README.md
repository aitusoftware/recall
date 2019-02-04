[![Build Status](https://travis-ci.org/aitusoftware/recall.svg)](https://travis-ci.org/aitusoftware/recall)

[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/aitusoftware/recall.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/aitusoftware/recall/context:java)

![Recall Icon](https://github.com/aitusoftware/recall/raw/master/resources/img/RecallIcon.png)

Recall is an off-heap, allocation-free object store for the JVM.

## Usage

Recall is designed for use in allocation-free or low-garbage systems. Objects are expected to be
mutable in order to reduce allocation costs. For this reason, domain objects should have
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

### Implement `Encoder`

```java
public class OrderEncoder implements Encoder<ByteBuffer, Order> {
  public void store(ByteBuffer buffer, int offset, Order order) {
    buffer.putLong(offset, order.getId());
    buffer.putDouble(offset + Long.BYTES, order.getQuantity());
    buffer.putDouble(offset + Long.BYTES + Double.BYTES, order.getPrice());
  }
}
```

### Implement `Decoder`

```java
public class OrderDecoder implements Decoder<ByteBuffer, Order> {
  public void load(ByteBuffer buffer, int offset, Order target) {
    target.setId(buffer.getLong(offset));
    target.setQuantity(buffer.getDouble(offset + Long.BYTES));
    target.setPrice(buffer.getDouble(offset + Long.BYTES + Double.BYTES));
  }
}
```

### Implement `IdAccessor`

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

### Storage and Retrieval

Domain objects can be serialised to off-heap storage, and retrieved at a later time:

```java
long orderId = 42L;
Order testOrder = new Order(orderId, 12.34D, 56.78D);
typeStore.store(testOrder);

Order container = new Order(-1, -1, -1);
assert typeStore.load(orderId, container);
assert container.getQuantity() == 12.34D;
```

## SBE integration

Recall is able to provide efficient off-heap storage of SBE-encoded messages.

This example uses the canonical `Car` example from
[SBE](https://github.com/real-logic/simple-binary-encoding/blob/master/sbe-tool/src/test/resources/example-schema.xml).

### SBE codecs

SBE objects must be generated with:

`-Dsbe.java.generate.interfaces=true`

this causes the `Decoder` to implement `MessageDecoderFlyweight`.

### Implement `IdAccessor`

It is necessary to implement the `IdAccessor` interface for the SBE `Decoder` type:

```java
public class CarIdAccessor implements IdAccessor<CarDecoder> {
  public long getId(CarDecoder decoder) {
    return decoder.id();
  }
}
```

### SBE Message Store

Create a `SingleTypeStore` for the type of the `Decoder`:

```java
SingleTypeStore<UnsafeBuffer, CarDecoder> messageStore =
  SbeMessageStoreFactory.forSbeMessage(new CarDecoder(),
    MAX_RECORD_LENGTH, 100,
    len -> new UnsafeBuffer(ByteBuffer.allocateDirect(len)),
    new CarIdAccessor());
```

*Note*: it is up to the application developer to determine the maximum length
of any given SBE message (even in the case of variable-length fields).

If an encoded value exceeds the specified maximum record length, then the
`store` method will throw an `IllegalArgumentException`.

SBE messages can now be stored for later retrieval:

#### Storage

```java
public void receiveCar(ReadableByteChannel channel) {
  CarDecoder decoder = new CarDecoder();
  UnsafeBuffer buffer = new UnsafeBuffer();
  ByteBuffer inputData = ByteBuffer.allocateDirect();
  channel.read(inputData);
  inputData.flip();
  buffer.wrap(inputData);
  decoder.wrap(buffer, 0, blockLength, version);

  dispatchCarReceivedEvent(decoder);
  messageStore.store(decoder);
}
```

#### Retrieval

```java
public void notifyCarSold(long carId) {
  CarDecoder decoder = new CarDecoder();
  messageStore.load(carId, decoder);

  dispatchCarSoldEvent(decoder);
}
```

## Non-integer keys

Since it is sometimes useful to be able to store and retrieve objects by something other
than an integer key, Recall also provides the ability to create mappings based on
variable-length keys based on either strings, or byte-sequences.

### `CharSequenceMap`

`CharSequenceMap` is an open-addressed hash map with that can be used to store a `CharSequence`
against an integer identifier.

Example usage:

```java
private final OrderByteBufferTranscoder transcoder =
    new OrderByteBufferTranscoder();
private final SingleTypeStore<ByteBuffer, Order> store =
    new SingleTypeStore<>(new BufferStore<>(MAX_RECORD_LENGTH, INITIAL_SIZE,
        ByteBuffer::allocateDirect, new ByteBufferOps()),
        transcoder, transcoder, Order::getId);
private final CharSequenceMap orderBySymbol =
    new CharSequenceMap(MAX_KEY_LENGTH, INITIAL_SIZE);

private void execute()
{
    final String[] symbols = new String[INITIAL_SIZE];
    for (int i = 0; i < INITIAL_SIZE; i++)
    {
        final Order order = Order.of(i);

        store.store(order);
        orderBySymbol.insert(order.getSymbol(), order.getId());
        symbols[i] = order.getSymbol().toString();
    }

    final Order container = Order.of(-1L);
    final AtomicInteger matchCount = new AtomicInteger();
    for (int i = 0; i < INITIAL_SIZE; i++)
    {
        final String searchTerm = symbols[i];
        orderBySymbol.search(searchTerm, id -> {
            store.load(id, container);
            matchCount.incrementAndGet();
            System.out.printf("Order with symbol %s has id %d%n", searchTerm, id);
        });
    }

    if (matchCount.get() != INITIAL_SIZE)
    {
        throw new IllegalStateException();
    }
}
```