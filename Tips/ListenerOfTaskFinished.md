```scala
  Dataset<Row1> events = rawEvent
          .mapPartitions((MapPartitionsFunction<Row0, Row1>)iterator ->
          {
              TaskContext.get().addTaskCompletionListener(new TaskCompletionListener() {
                  @Override
                  public void onTaskCompletion(TaskContext context)
                  {
                      //do your work
                  }
              });

              if (!iterator.hasNext())
              {
                  return Collections.emptyIterator();
              }
              ...
         }

```
