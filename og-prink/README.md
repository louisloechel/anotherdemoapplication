# Privacy Preserving Flink

## A fully functional data anonymization solution for Apache Flink
Prink is a data anonymization solution for Apache Flink, that provides k-anonymity and l-diversity for data streams. 
It implements the [CASTLE library](https://ieeexplore.ieee.org/document/5374415) providing strong anonymization guarantees for numerical and non-numerical data, as well as support for multiple sensitive attributes and custom generalizers.

## Getting started
- Install Apache Flink following the official installation [instructions](https://nightlies.apache.org/flink/flink-docs-release-1.15//docs/try-flink/local_installation/).
- Clone repository

## Usage
To use Prink add the `CastleFunction` as a process into your data stream using `.process()`:

	.process(new CastleFunction())

Make sure to key your data stream by the user identifier using the `.keyBy()` function (in the example below `user_id` is at field position 0):

	.keyBy(dataTuple -> dataTuple.getField(0))

Add a `ruleBroadcastStream` using the `.connect()` function to be able to communicate with Prink and to set rules:

	.connect(ruleBroadcastStream)

You can use your own implementation for the `ruleBroadcastStream` or if you only want to transfer some rules at startup a collection generated stream, as seen below.
Prink uses a `MapState` for rule broascasting:

	MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor =
                new MapStateDescriptor<>(
                        "RulesBroadcastState",
                        BasicTypeInfo.INT_TYPE_INFO,
                        TypeInformation.of(new TypeHint<CastleRule>(){}));

    ArrayList<CastleRule> rules = new ArrayList<>();
        rules.add(new CastleRule(0, new ReductionGeneralizer(), false));
        rules.add(new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(17f, 90f)), false));
        rules.add(new CastleRule(2, new NoneGeneralizer(), true));


	BroadcastStream<CastleRule> ruleBroadcastStream = env
                .fromCollection(rules)
                .broadcast(ruleStateDescriptor);

Currently, it is needed to specify the return type of the data stream. Use the `.returns()` function to do so (change the `Tuple` size to the required size of your project).

	.returns(TypeInformation.of(new TypeHint<Tuple4<Object, Object, Object, Object>>(){}));

The complete Prink integration should look something like this (excluding the rule broadcast stream):

	DataStream<Tuple4<Object, Object, Object, Object>> myDataStream = env
                .addSource(new MySource())

	DataStream<Tuple4<Object, Object, Object, Object>> myPrinkDataStream = myDataStream
                .keyBy(dataTuple -> dataTuple.getField(0))
                .connect(ruleBroadcastStream)
                .process(new CastleFunction())
                .returns(TypeInformation.of(new TypeHint<Tuple4<Object, Object, Object, Object,>>(){}));

To run your Flink job follow the offical Flink [documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/cli/) or if you use the local installation run the following commands.

To start your Flink cluster run:

    $ ./bin/start-cluster.sh

To start your Flink job run:

	$ ./bin/flink run <your-flink-job>.jar

To stop your Flink cluster run:

	$ ./bin/stop-cluster.sh

---

To change the parameters of Prink add them when creating the `CastleFunction`:

	new CastleFunction(0, 50, 2, 200, 50, 50, 50, true, 2)

The parameters are the following:

| Name             | Type    | Default | Description                                                                                     |
|------------------|---------|---------|-------------------------------------------------------------------------------------------------|
| posTupleId       | int     | 0       | Position of the id value inside the tuples                                                      |
| k                | int     | 5       | Value k for k-anonymity                                                                         |
| l                | int     | 2       | Value l for l-diversity (if 0 l-diversity is not applied)                                       |
| delta            | int     | 20      | Number of 'delayed' tuples                                                                      |
| beta             | int     | 50      | Max number of clusters in bigGamma                                                              |
| zeta             | int     | 10      | Max number of clusters in bigOmega                                                              |
| mu               | int     | 10      | Number of last infoLoss values considered for tau                                               |
| showInfoLoss     | boolean | false   | If true Prink adds the info loss of a tuple at the end of the tuple (tuple size increases by 1) |
| suppressStrategy | int     | 2       | Defines how to handle tuples that need to be suppressed.                                        |

## Prink Rules
For the anonymization of the data stream Prink uses `CastleRules` to define how each data attribute should be handeld. These rules are broadcasted to the `CastleFunction` process and have the following structure:

| Name                   | Type                          | Allowed Values       | Usage                                                                    |
|------------------------|-------------------------------|----------------------|--------------------------------------------------------------------------|
| position               | int                           | 0-max size of Tuple  | Defines the position inside the data tuple that the rule should apply to |
| generalizer            | BaseGeneralizer               | BaseGeneralizer instance | Defines how the attribute at the defined position should be generalized |
| isSensibleAttribute    | boolean                       | [true, false]        | Defines if the position inside the data tuple is a sensible attribute    |
| infoLossMultiplier (Optional)| double                  | 0.0 - 1.0                  | Defines the multiplier for the Normalized Certainty Penalty calculation. If all rule values sum up to 1 the Normalized Certainty Penalty is used |

## Prink Generalizers
Currently, Prink provides the following Generalizers:

| Generalizer Class                   | Example Rule                                                                                                     | Generalization Result                                  |
|-------------------------------------|------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|
| NoneGeneralizer                     | `new CastleRule(5, new NoneGeneralizer(), false)`                               | `[20, 22, 35]` &#8594; `[20, 22, 35]`              |
| ReductionGeneralizer                | `new CastleRule(5, new ReductionGeneralizer(), false)`                          | `[123456, 12789, 12678]` &#8594; `"12****"`           |
| AggregationIntegerGeneralizer       | `new CastleRule(5, new AggregationIntegerGeneralizer(Tuple2.of(0, 100)), false)` | `[20, 22, 35]` &#8594; `(20, 35)`                    |
| AggregationFloatGeneralizer         | `new CastleRule(5, new AggregationFloatGeneralizer(Tuple2.of(0f, 100f)), false)` | `[2.0f, 2.2f, 3.5f]` &#8594; `(2.0f, 3.5f)`          |
| AggregationDoubleGeneralizer        | `new CastleRule(5, new AggregationDoubleGeneralizer(Tuple2.of(0d, 100d)), false)`| `[2.0d, 2.2d, 3.5d]` &#8594; `(2.0d, 3.5d)`          |
| NonNumericalGeneralizer             | `new CastleRule(5, new NonNumericalGeneralizer(treeEntries), false)`            | `["IT-Student", "Bio-Student"]` &#8594; `"Student"` |

### Non-Numerical-Generalizer
To use the `NonNumericalGeneralizer` a domain generalization hierarchy needs to be defined.

This can be done in two ways:

1. sending an `Array` (`["Europe", "West-Europe","Germany"]`) instead of a `String` (`"Germany"`) as the data tuples attribute (This option is **not** encouraged and should only be used if necessary!).
1. providing it in the rule definition (**Preferred option**):

```
    ArrayList<String[]> treeNation = new ArrayList<>();
    treeNation.add(new String[]{"Europe", "South-Europe","Greece"});
    treeNation.add(new String[]{"Europe", "South-Europe","Italy"});
    treeNation.add(new String[]{"Europe", "South-Europe","Portugal"});
    treeNation.add(new String[]{"Europe", "West-Europe","England"});
    treeNation.add(new String[]{"Europe", "West-Europe","Germany"});
    treeNation.add(new String[]{"Asia", "West-Asia","Iran"});

    new CastleRule(14, new NonNumericalGeneralizer(treeNation), false)
```

## Custom Generalizer
Prink provides the option to add your own custom generalizer to the algorithm. This enables the use of new generalization concepts and the adaption to specific project needs.

To integrate a new Generalizer create a new Generalizer class that implements the `BaseGeneralizer`-Interface.
```
    public class MyCustomGeneralizer implements BaseGeneralizer{
        ...
    }
```

The Interface consists of 6 functions, that need to be implemented and have the following function:

### clone()
```
    BaseGeneralizer clone(int position, Cluster owningCluster);
```
The `clone()` function is part of the [prototype pattern](https://en.wikipedia.org/wiki/Prototype_pattern) used by Prink to create generalizer instances for newly created data clusters. When a new `Cluster` is created the `clone()` function will be called on the original generalizer instance provided in the defined `CastleRule` returning a new instance of the generalizer that only lives inside the new cluster.
The `clone()` function also provides the new generalizer instance with the `position` of the attribute that needs to be generalized, as well as a references to the `Cluster` that "owns" the generalizer instance. This references also enables the generalizer to have access to all data tuples inside the cluster by calling `getAllEntries()` on the cluster. 

For more information see: `BaseGeneralizer.java`

### generalize()
```
    Tuple2<?, Float> generalize();
```
The `generalize()` function is called when a data tuple needs to be generalized. 
It returns a `Tuple2` where the first value of the tuple is the generalization result of this generalizer and the second value is the resulting information loss of the applied generalization.
The generalization result can have any data type specified by the custom generalizer implementation. The information loss needs to be returned as a `Float` between `0f` (no information loss) and `1f` (all information is lost).

Note: Even though the `generalize()` function is called for each data tuple individually, the data tuple is not given to the `generalize()` function. This is to protect and ensure that the generalization is always applied over all data tuples inside the cluster, so no data can leak from an individual data tuple by accident.

**Important:** The generalization needs to happen over all data tuples inside the "owning" cluster and only these data tuples (`cluster.getAllEntries()`).
The function result is **not** allowed to be changed by the `generalize(withTuple)` function!

### generalize(withTuples)
```
    Tuple2<?, Float> generalize(List<Tuple> withTuples);
```
The `generalize(withTuple)` function is called when the enlargement value (the increase in information loss through a new data tuple in a cluster) needs to be calculated. 
The function should behave the exact same way as the normal `generalize()` function, except that the provided `List<Tuple>` of data tuples also needs to be included in the generalization result, returning a new generalization result and a new information loss.

**Important:** This inclusion of additional data tuples is only temporary and is not allowed to have any effect outside of the function call. The normal `generalize()` is not allowed to be affected by this function!

### generalizeMax()
```
    Tuple2<?, Float> generalizeMax();
```
The `generalizeMax()` function is called when a data tuple needs to be suppressed and should not just be set to `null`. This function should return the result of the maximal applied generalization of this generalizer, resulting in an information loss of `1f`.

Example: The `ReductionGeneralizer`s `generalizeMax()` function returns: `"*"` and the `AggregationIntegerGeneralizer`s `generalizeMax()` function with an upper and lower bound of `[0, 100]` returns: `(0, 100)`.

### addTuple(newTuple) - removeTuple(removedTuple)
```
    void addTuple(Tuple newTuple);
    void removeTuple(Tuple removedTuple);
```
The `addTuple(Tuple newTuple)` and `removeTuple(Tuple removedTuple)` function are helper functions that get called when a new data tuple is added or removed from the "owning" cluster. They make it possible to extract some of the generalizer logic out of the `generalize()` function and remove unnecessary calls and iterations over `cluster.getAllEntries()`.

Note: The use of these two functions is completely optional.

### Further information
If you need more information on custom generalizer creation, take a look at the Interface class: `BaseGeneralizer.java` and the default Prink generalizer implementations: `ReductionGeneralizer.java`, `AggregationIntGeneralizer.java`, `NonNumericalGeneralizer.java`.

[comment]: <> (## Contributing)

[comment]: <> (*WIP*)

## DEBUG

If you encounter the following error when starting a flink cluster and using Cygwin:

    Improperly specified VM option 'MaxMetaspaceSize=268435456

Follow the instructions here: https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/deployment/resource-providers/standalone/overview/#windows-cygwin-users

See also for additional error: https://stackoverflow.com/questions/72132420/why-apache-flink-in-not-running-on-windows

## License
The project will be open source. *WIP*
