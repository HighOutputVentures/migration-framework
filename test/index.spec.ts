import { expect } from 'chai';
import sinon from 'sinon';
import R from 'ramda';
import Migration, { Task, TaskAdapter } from './../src/index';

class TestMigration<T> extends Migration<T> {
  public constructor(
    public apply: (task: T) => Promise<void>,
    public startTransaction: () => Promise<void>,
    public commitTransaction: () => Promise<void>,
    public rollbackTransaction: () => Promise<void>,
    taskAdapter: TaskAdapter<T>) {
    super(taskAdapter);
  }
}

function makeTaskAdapter(tasks: Task<number>[]): TaskAdapter<number> {
  return {
    add: async (id, task) => {
      tasks.push({ id, task, status: 'PENDING' });
      return true;
    },
    take: async (n) => tasks.filter(t => t.status === 'PENDING' || t.status === 'FAILED').slice(0, n),
    update: async (id, status) => {
      tasks.filter((task) => task.id === id)[0].status = status;
    },
    clear: async () => {
      tasks = [];
    },
  }
}

describe('Migration', function() {
  beforeEach(async function() {
    this.tasks = [];
    this.adapter = makeTaskAdapter(this.tasks);

    R.repeat(R.identity, 5).map((_, i) => this.adapter.add(i, i))

    const sandbox = sinon.createSandbox();
    this.sandbox = sandbox;

    this.applyStub = sandbox.stub().resolves();
    this.startTransactionStub = sandbox.stub().resolves();
    this.commitTransactionStub = sandbox.stub().resolves();
    this.rollbackTransactionStub = sandbox.stub().resolves();

    this.addSpy = sandbox.spy(this.adapter, 'add');
    this.takeSpy = sandbox.spy(this.adapter, 'take');
    this.updateSpy = sandbox.spy(this.adapter, 'update');
    this.clearSpy = sandbox.spy(this.adapter, 'clear');

    this.instance = new TestMigration(
      this.applyStub,
      this.startTransactionStub,
      this.commitTransactionStub,
      this.rollbackTransactionStub,
      this.adapter,
    );
  });

  afterEach(function () {
    this.sandbox.restore();
  });

  describe('#process', function() {
    describe('GIVEN apply returns successfully', function() {
      beforeEach(async function() {
        await this.instance.process();
      });

      it('SHOULD call start transaction', function() {
        expect(this.startTransactionStub.callCount).to.equal(5);
      });

      it('SHOULD mark all the tasks as updated', function () {
        expect(this.tasks.filter(R.propEq('status', 'SUCCESS'))).to.have.lengthOf(5);
      });

      it('SHOULD commit the transaction', function() {
        expect(this.commitTransactionStub.callCount).to.equal(5);
      });
    });

    describe('GIVEN fails some task', function() {
      beforeEach(async function () {
        this.error = new Error();

        this.applyStub.resetBehavior();

        this.applyStub
          .onFirstCall()
          .resolves()
          .onSecondCall()
          .rejects(this.error)

        // suppress throwing of error
        await this.instance.process(2).catch(R.identity);
      });

      it('SHOULD only apply twice', function() {
        expect(this.applyStub.calledTwice).to.be.true;
      });

      it('SHOULD call the rollback', function () {
        expect(this.rollbackTransactionStub.calledOnce).to.be.true;
      });
    });
  });
});