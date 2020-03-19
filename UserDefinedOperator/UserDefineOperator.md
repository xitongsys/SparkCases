# User defined operator in Spark

In my usage, I don't need the shuffle and sort node before `flatMapGroupsWithState`. So I create the `flatMapGroupsWithStateWithoutShuffleSort`.

The code changes you can found [here]().

You should compile you codes and you can use your new defined operators.

If you run your codes in platform not handled by yourself. You should achieve your code and using following configs:
```json
"spark.driver.userClassPathFirst": true,
"spark.executor.userClassPathFirst": true
```