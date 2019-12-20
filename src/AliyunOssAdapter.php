<?php
/**
 * Flysystem adapter for aliyun OSS
 *
 */

namespace Atlasman\Flysystem\AliyunOss;

use League\Flysystem\Adapter\AbstractAdapter;
use League\Flysystem\AdapterInterface;
use League\Flysystem\Adapter\Polyfill\NotSupportingVisibilityTrait;
use League\Flysystem\Config;
use League\Flysystem\Util\MimeType;
use League\Flysystem\Exception;
use League\Flysystem\NotSupportedException;
use OSS\OssClient;
use OSS\Core\OssException;
use DateTimeInterface;

class AliyunOssAdapter extends AbstractAdapter
{
    const OSS_ACL_TYPE_DEFAULT = 'default';

    /**
     * @var OSS\OssClient
     */
    protected $client;

    /**
     * @var string
     */
    protected $bucket;

     /**
     * @var array
     */
    protected static $metaOptions = [];

    /**
     * 授权时间，单位：s 秒
     * @var integer
     */
    protected $expires = 3600;

    /**
     * Constructor
     * @param OssClient $client  [description]
     * @param string    $bucket  [description]
     * @param string    $prefix  [description]
     * @param array     $options [description]
     */
    public function __construct(OssClient $client, string $bucket, string $prefix = '', array $options = [])
    {
        $this->client = $client;
        $this->bucket = $bucket;
        $this->setPathPrefix($prefix);

        foreach ($options as $key => $val) {
            if (in_array($key, ['expires'])) {
                $this->$key = $val;
            }
        }
    }

    /**
     * Check whether a file exists.
     *
     * @param string $path
     *
     * @return array|bool|null
     */
    public function has($path)
    {
        try {
            $object = $this->applyPathPrefix($path);
            return $this->client->doesObjectExist($this->bucket, $object);
        } catch (OssException $e) {
            throw new Exception($e->getErrorMessage());
        }
    }

    /**
     * Read a file.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function read($path)
    {
        try {
            $object = $this->applyPathPrefix($path);
            $contents = $this->client->getObject($this->bucket, $object);
            return compact('contents');
        } catch (OssException $e) {
            throw new Exception($e->getErrorMessage());
        }
    }

    /**
     * Read a file as a stream.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function readStream($path)
    {
        $object = $this->applyPathPrefix($path);
        $url = $this->client->signUrl($this->bucket, $object, 3600);
        $stream = fopen($url, 'r');
        return compact('stream');
    }

    /**
     * List contents of a directory.
     *
     * @param string $directory
     * @param bool   $recursive
     *
     * @return array
     */
    public function listContents($directory = '', $recursive = false)
    {
        try {
            $prefix = $this->applyPathPrefix($directory);
            $contents     = [];
            $nextMarker = '';
            while (true) {
                // max-keys 用于限定此次返回object的最大数，如果不设定，默认为100，max-keys取值不能大于1000。
                // prefix   限定返回的object key必须以prefix作为前缀。注意使用prefix查询时，返回的key中仍会包含prefix。
                // delimiter是一个用于对Object名字进行分组的字符。所有名字包含指定的前缀且第一次出现delimiter字符之间的object作为一组元素
                // marker   用户设定结果从marker之后按字母排序的第一个开始返回。
                $options = [
                    'max-keys'  => 1000,
                    'prefix'    => $prefix,
                    'delimiter' => '/',
                    'marker'    => $nextMarker,
                ];
                $result = $this->client->listObjects($this->bucket, $options);

                $nextMarker = $result->getNextMarker();
                $prefixList = $result->getPrefixList(); // 目录列表
                $objectList = $result->getObjectList(); // 文件列表

                if ($prefixList) {
                    foreach ($prefixList as $value) {
                        $contents[] = [
                            'type' => 'dir',
                            'path' => $value->getPrefix()
                        ];
                        if ($recursive) {
                            $contents = array_merge($contents, $this->listContents($value->getPrefix(), $recursive));
                        }
                    }
                }
                if ($objectList) {
                    foreach ($objectList as $value) {
                        if (($value->getSize() === 0) && ($value->getKey() === $directory . '/')) {
                            continue;
                        }
                        $contents[] = [
                            'type'      => 'file',
                            'path'      => $value->getKey(),
                            'timestamp' => strtotime($value->getLastModified()),
                            'size'      => $value->getSize()
                        ];
                    }
                }
                if ($nextMarker === '') {
                    break;
                }
            }
            return $contents;
        } catch (OssException $e) {
            return [];
        }
    }

    /**
     * Get all the meta data of a file or directory.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getMetadata($path)
    {
        try {
            $object = $this->applyPathPrefix($path);
            $objectMeta = $this->client->getObjectMeta($this->bucket, $object);
            return $objectMeta;
        } catch (OssException $e) {
            throw new Exception($e->getErrorMessage());
        }
    }

    /**
     * Get the size of a file.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getSize($path)
    {
        $objectMeta = $this->getMetadata($path);
        return ['size' => $objectMeta['content-length']];
    }

    /**
     * Get the mimetype of a file.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getMimetype($path)
    {
        $objectMeta = $this->getMetadata($path);
        return ['mimetype' => $objectMeta['content-type']];
    }

    /**
     * Get the last modified time of a file as a timestamp.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getTimestamp($path)
    {
        $objectMeta = $this->getMetadata($path);
        return ['timestamp' => strtotime($objectMeta['last-modified'])];
    }

    /**
     * Get the visibility of a file.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getVisibility($path)
    {
        try {
            $object = $this->applyPathPrefix($path);
            $response = $this->client->getObjectAcl($this->bucket, $object);
            if ($response == self::OSS_ACL_TYPE_DEFAULT) {
                $response = $this->client->getBucketAcl($this->bucket);
            }
            $visibility = $response == OssClient::OSS_ACL_TYPE_PRIVATE? AdapterInterface::VISIBILITY_PRIVATE : AdapterInterface::VISIBILITY_PUBLIC;
            return compact('visibility');
        } catch (OssException $e) {
            throw new Exception($e->getErrorMessage());
        }
    }

    /**
     * Write a new file.
     *
     * @param string $path
     * @param string $contents
     * @param Config $config   Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function write($path, $contents, Config $config)
    {
        try {
            $object = $this->applyPathPrefix($path);
            $result = $this->client->putObject($this->bucket, $object, $contents, $this->getOptionsFromConfig($config));
            return $result;
        } catch (OssException $e) {
            throw new Exception($e->getErrorMessage());
        }
    }

    /**
     * Write a new file using a stream.
     *
     * @param string   $path
     * @param resource $resource
     * @param Config   $config   Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function writeStream($path, $resource, Config $config)
    {
        if ( ! is_resource($resource)) {
            throw new Exception(__METHOD__ . ' expects argument #2 to be a valid resource.');
        }
        try {
            $object = $this->applyPathPrefix($path);
            $i          = 0;
            $bufferSize = 1000000; // 1M
            while (!feof($resource)) {
                if (false === $buffer = fread($resource, $block = $bufferSize)) {
                    return false;
                }
                $position = $i * $bufferSize;
                $size     = $this->client->appendObject($this->bucket, $object, $buffer, $position, $this->getOptionsFromConfig($config));
                $i++;
            }
            fclose($resource);
            return true;
        } catch (OssException $e) {
            throw new Exception($e->getErrorMessage());
        }
    }

    /**
     * Update a file.
     *
     * @param string $path
     * @param string $contents
     * @param Config $config   Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function update($path, $contents, Config $config)
    {
        try {
            $object = $this->applyPathPrefix($path);
            $result = $this->oss->putObject($this->bucket, $object, $contents, $this->getOptionsFromConfig($config));
            return $result;
        } catch (OssException $e) {
            throw new Exception($e->getErrorMessage());
        }
    }

    /**
     * Update a file using a stream.
     *
     * @param string   $path
     * @param resource $resource
     * @param Config   $config   Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function updateStream($path, $resource, Config $config)
    {
        $result = $this->write($path, stream_get_contents($resource), $config);
        if (is_resource($resource)) {
            fclose($resource);
        }
        return $result;
    }

    /**
     * Rename a file.
     *
     * @param string $path
     * @param string $newpath
     *
     * @return bool
     */
    public function rename($path, $newpath)
    {
        try {
            $path = $this->applyPathPrefix($path);
            $newpath = $this->applyPathPrefix($newpath);
            $this->client->copyObject($this->bucket, $path, $this->bucket, $newpath);
            $this->client->deleteObject($this->bucket, $path);
            return true;
        } catch (OssException $e) {
            return false;
        }
    }

    /**
     * Copy a file.
     *
     * @param string $path
     * @param string $newpath
     *
     * @return bool
     */
    public function copy($path, $newpath)
    {
        try {
            $fromObject = $this->applyPathPrefix($path);
            $toObject = $this->applyPathPrefix($newpath);
            $this->client->copyObject($this->bucket, $fromObject, $this->bucket, $toObject);
            return true;
        } catch (OssException $e) {
            throw new Exception($e->getErrorMessage());
        }
    }

    /**
     * Delete a file.
     *
     * @param string $path
     *
     * @return bool
     */
    public function delete($path)
    {
        try {
            $object = $this->applyPathPrefix($path);
            return $this->client->deleteObject($this->bucket, $object);
        } catch (OssException $e) {
            throw new Exception($e->getErrorMessage());
        }
    }

    /**
     * Delete a directory.
     *
     * @param string $dirname
     *
     * @return bool
     */
    public function deleteDir($dirname)
    {
        return $this->delete($dirname);
    }

    /**
     * Create a directory.
     *
     * @param string $dirname directory name
     * @param Config $config
     *
     * @return array|false
     */
    public function createDir($dirname, Config $config)
    {
        throw new NotSupportedException('This driver does not support create directory');
    }

    /**
     * Set the visibility for a file.
     *
     * @param string $path
     * @param string $visibility
     *
     * @return array|false file meta data
     */
    public function setVisibility($path, $visibility)
    {
        try {
            $object = $this->applyPathPrefix($path);
            $acl = $visibility == AdapterInterface::VISIBILITY_PUBLIC ? OssClient::OSS_ACL_TYPE_PUBLIC_READ : OssClient::OSS_ACL_TYPE_PRIVATE;
            return $this->client->putObjectAcl($this->bucket, $object, $acl);
        } catch (OssException $e) {
            throw new Exception($e->getErrorMessage());
        }
    }

    /**
     * [getUrl description]
     * @param  [type] $path [description]
     * @return [type]       [description]
     */
    public function getUrl($path)
    {
        return $this->publicUrl($path);
    }

    /**
     * Get a temporary URL for the file at the given path.
     *
     * @param  string  $path
     * @param  DateTimeInterface\null  $expiration
     * @param  array  $options
     * @return string
     *
     * @throws \RuntimeException
     */
    public function getTemporaryUrl(string $path, DateTimeInterface $expiration = null, array $options = []): string
    {
        try {
            $object = $this->applyPathPrefix($path);
            if (!empty($expiration)) {
                $expires = $expiration->getTimestamp() - time();
            } else {
                $expires = $this->expires;
            }
            return $this->client->signUrl(
                $this->bucket,
                $object,
                $expires,
                OssClient::OSS_HTTP_GET,
                $options
            );
        } catch (OssException $e) {
            throw new Exception($e->getErrorMessage());
        }
    }

    /**
     * [publicUrl description]
     * @param  string $path    [description]
     * @param  array  $options [description]
     * @return [type]          [description]
     */
    public function publicUrl(string $path, array $options = [])
    {
        try {
            $url = $this->getTemporaryUrl($path, null, $options);
            $parse = parse_url($url);
            $query = explode('&', $parse['query']);
            $query = array_filter($query, function($item){
                list($key, $val) = explode('=', $item);
                if (!in_array($key, ['OSSAccessKeyId', 'Expires', 'Signature'])) {
                    return $item;
                }
            });
            $baseUrl = $parse['scheme'].'://'.$parse['host'].$parse['path'];
            return empty($query)? $baseUrl : $baseUrl.'?'.implode('&', $query);
        } catch (Exception $e) {
            throw $e;
        }
    }

    /**
     * Get options from the config.
     *
     * @param Config $config
     *
     * @return array
     */
    protected function getOptionsFromConfig(Config $config)
    {
        $options =[];

        if ($config->has("headers")) {
            $options['headers'] = $config->get("headers");
        }
        if ($config->has("Content-Type")) {
            $options["Content-Type"] = $config->get("Content-Type");
        }
        if ($config->has("Content-Md5")) {
            $options["Content-Md5"] = $config->get("Content-Md5");
            $options["checkmd5"]    = false;
        }

        if ($visibility = strtolower($config->get('visibility'))) {
            $options['headers']['x-oss-object-acl'] = $visibility == AdapterInterface::VISIBILITY_PUBLIC ? 'public-read' : 'private';
        }

        foreach (static::$metaOptions as $option) {
            if ($config->has($option)) {
                $options[$option] = $config->get($option);
            }
        }
        return $options;
    }
}