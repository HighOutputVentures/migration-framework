import Bluebird from 'bluebird';
import prettyMs from 'pretty-ms';
import chalk from 'chalk';
import prettyStringify from 'json-stringify-pretty-compact';

export type TaskStatus = 'PENDING' | 'FAILED' | 'SUCCESS';

type ID = string | Buffer | number;

export interface Task<T> {
  id: ID;
  task: T;
  status: TaskStatus;
}

export interface TaskAdapter<T> {
  /**
   * Clears the previously added tasks
   */
  clear(): Promise<void>;

  /**
   * Retrieve n number of `PENDING` | `FAILED` task(s).
   * @param n
   */
  take(n: number): Promise<Task<T>[]>;

  /**
   * Add a new task with the given id and task that has a status of `PENDING`
   * @param id
   * @param task
   */
  add(id: ID, task: T): Promise<boolean>;

  /**
   * Updates the status of the task.
   * @param id
   * @param status
   */
  update(id: ID, status: TaskStatus): Promise<void>;
}

export abstract class Migration<T> {
  constructor(private taskAdapter: TaskAdapter<T>) {}

  abstract apply(task: T): Promise<void>;
  abstract startTransaction(): Promise<void>;
  abstract commitTransaction(): Promise<void>;
  abstract rollbackTransaction(): Promise<void>;

  async process(concurrency: number = 1) {
    let tasks: Task<T>[] = await this.taskAdapter.take(concurrency);
    let errors: { task: ID; error: Error }[] = [];
    let totalProcessed = 0;
    
    const reportIntervalHandle = setInterval(() => {
      console.log(chalk`{yellow Total processed record: ${totalProcessed}}`);
    }, 5000);

    const startTime = Date.now();

    while (tasks.length > 0) {
      await this.startTransaction();

      await Bluebird.map(
        tasks,
        (task) =>
          this.apply(task.task).catch(
            async (error) => errors.push({ task: task.id, error }),
          ),
      );

      if (errors.length > 0) {
        console.log(chalk`{red Migration encountered some errors:}`);
        console.log(chalk`{red ${prettyStringify(errors)}}`);

        this.rollbackTransaction();
        return;
      }

      await Bluebird.map(
        tasks,
        (task) => this.taskAdapter.update(task.id, 'SUCCESS'),
      );

      await this.commitTransaction();

      totalProcessed += tasks.length;
      tasks = await this.taskAdapter.take(concurrency);
    };

    clearInterval(reportIntervalHandle);
    console.log(chalk`{green Migration successfully finished after ${prettyMs(Date.now() - startTime)}.}`);
    console.log(chalk`{green Total records migrated: ${totalProcessed}}`);
  }
}

export default Migration;
