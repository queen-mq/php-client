<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\Response;
use Illuminate\Container\Container;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Queen\Http\PopFrames;
use Queen\Http\Zstd;
use Queen\Laravel\Dashboard\ApplicationSettings;
use Queen\Laravel\QueenServiceProvider;
use Queen\Laravel\Queue\QueenConnector;
use Queen\Laravel\Queue\QueenJob;
use Queen\Laravel\Queue\QueenQueue;

/**
 * The client's wire forms in the Laravel driver: binary_push, push_compression
 * and framed_pop of config/queen.php (or of one queue connection).
 *
 * The driver pushes, pops and commits through its Queen client, so the three
 * settings are the client's options and nothing else. What is at stake: off,
 * which is the default, a worker and a dispatcher put the JSON requests of
 * every released broker on the wire; on, a job goes out and comes back as the
 * same Laravel payload.
 *
 * push_compression and framed_pop need the zstd extension; their tests are
 * skipped where it is not loaded.
 */
class LaravelWireFormsTest extends TestCase
{
    /** @var list<RequestInterface> */
    private array $requests = [];

    /**
     * A queue whose requests are answered, in order, by $answers: each a
     * [status, content type, body] triple, the last one repeated.
     *
     * @param list<array{0: int, 1: string, 2: string}> $answers
     */
    private function queueFor(array $answers, array $connection = []): QueenQueue
    {
        $this->requests = [];
        $handler = function (RequestInterface $request, array $options) use (&$answers): PromiseInterface {
            $this->requests[] = $request;
            [$status, $contentType, $body] = count($answers) > 1 ? array_shift($answers) : $answers[0];

            return new FulfilledPromise(new Response($status, ['Content-Type' => $contentType], $body));
        };
        $queue = (new QueenConnector())->connect(array_replace([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
            'queue' => 'default',
            'consumer_group' => 'workers',
            'partitions' => 8,
            'partition_prefix' => 'job',
            'retry_after' => 120,
            'block_for' => 0,
        ], $connection));
        $queue->setContainer(new Container());
        $queue->setConnectionName('queen');

        return $queue;
    }

    private static function json(mixed $body, int $status = 200): array
    {
        return [$status, 'application/json', json_encode($body, JSON_THROW_ON_ERROR)];
    }

    private static function queued(): array
    {
        return self::json([['status' => 'queued']], 201);
    }

    /** The Laravel payload of a job, as Queue::createPayload() builds it. */
    private static function payload(string $uuid): array
    {
        return [
            'uuid' => $uuid,
            'displayName' => 'Handler',
            'job' => 'Handler@handle',
            'maxTries' => null,
            'maxExceptions' => null,
            'failOnTimeout' => false,
            'backoff' => null,
            'timeout' => null,
            'data' => ['text' => "à \"quoted\"\nline"],
            'createdAt' => 1_700_000_000,
        ];
    }

    /** The JSON pop answer of one job of queue `emails`. */
    private static function popAnswer(array $payload): array
    {
        return self::json([
            'success' => true,
            'queue' => 'emails',
            'leaseId' => 'lease-1',
            'consumerGroup' => 'workers',
            'messages' => [[
                'id' => 'message-1',
                'transactionId' => 'transaction-1',
                'partitionId' => '7',
                'partition' => 'job-0001',
                'leaseId' => 'lease-1',
                'consumerGroup' => 'workers',
                'deliveryAttempt' => 1,
                'data' => $payload,
            ]],
        ]);
    }

    private static function text(string $text): string
    {
        return pack('v', strlen($text)) . $text;
    }

    /**
     * The framed pop answer of the same job: one claim on partition 7
     * ("job-0001") of `emails`, one zstd record holding the one frame.
     */
    private static function framedPopAnswer(array $payload): array
    {
        $body = "\0" . "\x01" . str_repeat("\0", 5) . "\x70\0\x80" . str_repeat("\0", 7)
            . self::text('transaction-1') . json_encode($payload, JSON_THROW_ON_ERROR);
        $record = Zstd::compress(pack('V', strlen($body)) . $body);
        $own = '{"frames":true}';

        return [200, PopFrames::CONTENT_TYPE, "QPF1\x01" . pack('V', strlen($own)) . $own
            . self::text('emails') . self::text('lease-1') . self::text('workers')
            . pack('V', 1)
            . pack('P', 7) . self::text('job-0001') . pack('P', 0) . pack('P', 0)
            . pack('V', 1)                      // delivery attempt
            . pack('V', 1)                      // records
            . pack('P', 0) . pack('P', 1_700_000_000_000_000) . pack('V', 1)
            . "\x01" . pack('V', strlen($record)) . $record];
    }

    private static function u16(string $bytes, int &$at): int
    {
        $at += 2;

        return unpack('v', substr($bytes, $at - 2, 2))[1];
    }

    private static function u32(string $bytes, int &$at): int
    {
        $at += 4;

        return unpack('V', substr($bytes, $at - 4, 4))[1];
    }

    private static function take(string $bytes, int &$at, int $length): string
    {
        $at += $length;

        return substr($bytes, $at - $length, $length);
    }

    /**
     * The one group of framed messages at the end of a binary body:
     * [queue, partition, transaction ids, decoded payloads].
     *
     * @return array{0: string, 1: string, 2: list<string>, 3: list<array>}
     */
    private static function framedGroup(string $bytes): array
    {
        $at = 0;
        self::assertSame(1, self::u32($bytes, $at), 'one group');
        $queue = self::take($bytes, $at, self::u16($bytes, $at));
        $partition = self::take($bytes, $at, self::u16($bytes, $at));
        $count = self::u32($bytes, $at);
        $frames = Zstd::uncompress(self::take($bytes, $at, self::u32($bytes, $at)));
        self::assertSame(strlen($bytes), $at, 'bytes after the group');
        $transactionIds = [];
        $payloads = [];
        $in = 0;
        for ($i = 0; $i < $count; $i++) {
            $frame = self::take($frames, $in, self::u32($frames, $in));
            $p = 17;
            $transactionIds[] = self::take($frame, $p, self::u16($frame, $p));
            $payloads[] = json_decode(substr($frame, $p), true, 512, JSON_THROW_ON_ERROR);
        }

        return [$queue, $partition, $transactionIds, $payloads];
    }

    private static function stripeOf(string $uuid): string
    {
        return sprintf('job-%04d', hexdec(substr(hash('sha256', $uuid), 0, 8)) % 8);
    }

    public function testTheDriverSendsAndReadsJsonByDefault(): void
    {
        $queue = $this->queueFor([self::queued(), self::popAnswer(self::payload('job-123'))]);

        $this->assertSame('job-123', $queue->pushRaw(json_encode(self::payload('job-123')), 'emails'));
        $this->assertInstanceOf(QueenJob::class, $queue->pop('emails'));

        [$push, $pop] = $this->requests;
        $this->assertSame('application/json', $push->getHeaderLine('Content-Type'));
        $this->assertSame('{', ((string) $push->getBody())[0]);
        $this->assertStringNotContainsString('frames', $pop->getUri()->getQuery());
    }

    #[TestWith(['binary_push'])]
    #[TestWith(['binaryPush'])]
    public function testBinaryPushSendsADispatchedJobAsTheBinaryPushBody(string $key): void
    {
        $queue = $this->queueFor([self::queued()], [$key => true]);

        $this->assertSame('job-123', $queue->pushRaw(json_encode(self::payload('job-123')), 'emails'));

        $push = $this->requests[0];
        $this->assertSame('/api/v1/push', $push->getUri()->getPath());
        $this->assertSame('application/x-queen-push', $push->getHeaderLine('Content-Type'));
        $bytes = (string) $push->getBody();
        $this->assertSame("QPB1\x04", substr($bytes, 0, 5));
        // One group, one item: its names, the job's id as transaction id, the payload.
        $at = 5;
        $this->assertSame(1, self::u32($bytes, $at));
        $this->assertSame('emails', self::take($bytes, $at, self::u16($bytes, $at)));
        $this->assertSame(self::stripeOf('job-123'), self::take($bytes, $at, self::u16($bytes, $at)));
        $this->assertSame(1, self::u32($bytes, $at));
        $this->assertSame('job-123', self::take($bytes, $at, self::u16($bytes, $at)));
        $payload = json_decode(self::take($bytes, $at, self::u32($bytes, $at)), true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame(strlen($bytes), $at);
        $this->assertSame(['Handler@handle', self::payload('job-123')['data'], 0], [$payload['job'], $payload['data'], $payload['_queen']['attempts']]);
    }

    #[RequiresPhpExtension('zstd')]
    public function testPushCompressionSendsADispatchedJobAsOneCompressedFrame(): void
    {
        $queue = $this->queueFor([self::queued()], ['binary_push' => true, 'push_compression' => true]);

        $this->assertSame('job-123', $queue->pushRaw(json_encode(self::payload('job-123')), 'emails'));

        $bytes = (string) $this->requests[0]->getBody();
        $this->assertSame("QPB1\x08", substr($bytes, 0, 5));
        [$name, $partition, $transactionIds, $payloads] = self::framedGroup(substr($bytes, 5));
        $this->assertSame(['emails', self::stripeOf('job-123'), ['job-123']], [$name, $partition, $transactionIds]);
        $this->assertSame(self::payload('job-123')['data'], $payloads[0]['data']);
    }

    #[RequiresPhpExtension('zstd')]
    public function testFramedPopHandsTheWorkerTheJobOfAFramedAnswer(): void
    {
        $payload = self::payload('job-123');
        $fromJson = $this->queueFor([self::popAnswer($payload)])->pop('emails');

        $job = $this->queueFor([self::framedPopAnswer($payload)], ['framed_pop' => true])->pop('emails');

        parse_str($this->requests[0]->getUri()->getQuery(), $query);
        $this->assertSame('true', $query['frames']);
        $this->assertInstanceOf(QueenJob::class, $job);
        $this->assertSame('job-123', $job->getJobId());
        $this->assertSame($fromJson->payload(), $job->payload());
        $this->assertSame([$fromJson->attempts(), $fromJson->getQueue()], [$job->attempts(), $job->getQueue()]);
    }

    #[RequiresPhpExtension('zstd')]
    public function testAReleasedJobTravelsInTheBinaryTransactionBody(): void
    {
        $queue = $this->queueFor(
            [self::popAnswer(self::payload('job-123')), self::json(['success' => true, 'transactionId' => 'bundle-1', 'results' => []])],
            ['binary_push' => true, 'push_compression' => true],
        );

        $queue->pop('emails')->release();

        $transaction = $this->requests[1];
        $this->assertSame('/api/v1/transaction', $transaction->getUri()->getPath());
        $this->assertSame('application/x-queen-push', $transaction->getHeaderLine('Content-Type'));
        $bytes = (string) $transaction->getBody();
        $this->assertSame("QTB1\x00", substr($bytes, 0, 5));
        $headLength = unpack('V', substr($bytes, 5, 4))[1];
        $head = json_decode(substr($bytes, 9, $headLength), true, 512, JSON_THROW_ON_ERROR);
        // The head: the ACK of the delivery, under its lease. The copy is not in it.
        $this->assertSame(['ack'], array_column($head['operations'], 'type'));
        $this->assertSame(['transaction-1', '7', 'workers', 'lease-1'], [
            $head['operations'][0]['transactionId'],
            $head['operations'][0]['partitionId'],
            $head['operations'][0]['consumerGroup'],
            $head['operations'][0]['leaseId'],
        ]);
        $this->assertSame(['lease-1'], $head['requiredLeases']);
        // The copy: one frame on the job's own partition, the attempt counted.
        [$name, $partition, $transactionIds, $payloads] = self::framedGroup(substr($bytes, 9 + $headLength));
        $this->assertSame(['emails', 'job-0001'], [$name, $partition]);
        $this->assertCount(1, $transactionIds);
        $this->assertSame(['job-123', 1], [$payloads[0]['uuid'], $payloads[0]['_queen']['attempts']]);
    }

    #[TestWith(['binary_push'])]
    #[TestWith(['push_compression'])]
    #[TestWith(['framed_pop'])]
    public function testTheConnectorRefusesAWireFormThatIsNotABoolean(string $key): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage("Queen Laravel {$key} must be a boolean.");

        $this->queueFor([self::queued()], [$key => 'true']);
    }

    public function testAnOptionThatNeedsZstdSaysSoWhereTheExtensionIsMissing(): void
    {
        if (Zstd::available()) {
            $this->assertInstanceOf(QueenQueue::class, $this->queueFor(
                [self::queued()],
                ['binary_push' => true, 'push_compression' => true, 'framed_pop' => true],
            ));

            return;
        }
        // push_compression alone compresses nothing: it is binary_push that sends frames.
        $this->assertInstanceOf(QueenQueue::class, $this->queueFor([self::queued()], ['push_compression' => true]));
        foreach ([['binary_push' => true, 'push_compression' => true], ['framed_pop' => true]] as $connection) {
            try {
                $this->queueFor([self::queued()], $connection);
                $this->fail('a connection that needs zstd was made without it');
            } catch (\RuntimeException $error) {
                $this->assertStringContainsString('zstd extension', $error->getMessage());
            }
        }
    }

    public function testTheContainersClientGetsTheWireFormsOfTheConfig(): void
    {
        $config = [
            'url' => 'http://queen.test:6632',
            'bearer_token' => null,
            'timeout' => 30000,
            'retry_attempts' => 3,
            'load_balancing_strategy' => 'affinity',
        ];
        $names = ['binaryPush', 'pushCompression', 'framedPop'];
        $pick = static fn (array $client): array => array_map(static fn (string $name): mixed => $client[$name], $names);

        $this->assertSame([false, false, false], $pick(QueenServiceProvider::clientConfig($config)));
        $this->assertSame([true, true, true], $pick(QueenServiceProvider::clientConfig(
            $config + ['binary_push' => true, 'push_compression' => true, 'framed_pop' => true],
        )));
    }

    public function testThePublishedConfigLeavesTheWireFormsOff(): void
    {
        $source = (string) file_get_contents(__DIR__ . '/../config/queen.php');

        foreach (['binary_push', 'push_compression', 'framed_pop'] as $key) {
            $this->assertMatchesRegularExpression("/^    '{$key}' => false,$/m", $source, $key);
        }
    }

    public function testTheDashboardShowsTheWireFormsAmongTheConnectionSettings(): void
    {
        $rows = static function (array $connection): array {
            $settings = new ApplicationSettings([
                'queen' => [],
                'queue' => ['connections' => ['queen' => ['driver' => 'queen', ...$connection]]],
            ]);

            return array_column($settings->rows()['connection'], null, 'name');
        };

        $off = $rows([]);
        $on = $rows(['binary_push' => true, 'push_compression' => true, 'framed_pop' => true]);
        foreach (['binary_push', 'push_compression', 'framed_pop'] as $name) {
            $this->assertSame([false, false], [$off[$name]['changed'], $off[$name]['invalid']], $name);
            $this->assertSame([true, false], [$on[$name]['changed'], $on[$name]['invalid']], $name);
            $this->assertNotSame($off[$name]['value'], $on[$name]['value'], $name);
        }
    }
}
