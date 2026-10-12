<?php

namespace Queen\Http;

/**
 * zstd for the binary push body and the framed pop answer.
 *
 * It comes from the zstd extension (`pecl install zstd`). The client asks for
 * it only when pushCompression or framedPop is on.
 */
final class Zstd
{
    public static function available(): bool
    {
        return \function_exists('zstd_compress') && \function_exists('zstd_uncompress');
    }

    /** @throws \RuntimeException when the extension is not loaded */
    public static function require(string $option): void
    {
        if (!self::available()) {
            throw new \RuntimeException("{$option} needs the zstd extension (pecl install zstd)");
        }
    }

    /**
     * One zstd frame of $raw, at the fastest level. The frame carries its
     * content size, which the broker asks of a frame it stores as it came.
     */
    public static function compress(string $raw): string
    {
        self::require('Compressing a push');
        $frame = \zstd_compress($raw, 1);
        if ($frame === false) {
            throw new \RuntimeException('zstd could not compress a push');
        }

        return $frame;
    }

    public static function uncompress(string $frame): string
    {
        self::require('Reading a compressed record');
        $raw = @\zstd_uncompress($frame);
        if ($raw === false) {
            throw new \UnexpectedValueException('zstd could not read a frame');
        }

        return $raw;
    }
}
