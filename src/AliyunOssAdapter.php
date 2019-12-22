<?php
/**
 * Flysystem adapter for aliyun OSS
 *
 */

namespace Atlasman\Flysystem\AliyunOss;

use League\Flysystem\Adapter\AbstractAdapter;
use League\Flysystem\AdapterInterface;
use League\Flysystem\Adapter\Polyfill\NotSupportingVisibilityTrait;
use League\Flysystem\Adapter\Polyfill\StreamedTrait;
use League\Flysystem\Config;
use League\Flysystem\Util\MimeType;
use League\Flysystem\Exception;
use League\Flysystem\NotSupportedException;
use OSS\OssClient;
use OSS\Core\OssException;
use DateTimeInterface;

class AliyunOssAdapter extends AbstractAdapter
{
    use StreamedTrait,
        NotSupportingVisibilityTrait;

    const VERSION = '1.0.3';

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
    protected $options = [];

     /**
     * @var array
     */
    protected static $metaOptions = [
        'mimetype' => OssClient::OSS_CONTENT_TYPE,
        'size'     => OssClient::OSS_LENGTH,
    ];

    /**
     * 默认acl
     * @var
     */
    protected $acl;

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

        if (isset($options['acl'])) {
            $this->acl = $options['acl'];
            unset($options['acl']);
        }
        $this->options = $options;
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
            $options = $this->getOptionsFromConfig($config);
            if ($this->acl && (empty($options['headers']) || empty($options['headers'][OssClient::OSS_OBJECT_ACL]) )) {
                $options['headers'][OssClient::OSS_OBJECT_ACL] = $this->acl;
            }
            $result = $this->client->putObject($this->bucket, $object, $contents, $options);
            return $result;
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
            $result = $this->client->putObject($this->bucket, $object, $contents, $this->getOptionsFromConfig($config));
            return $result;
        } catch (OssException $e) {
            throw new Exception($e->getErrorMessage());
        }
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
            throw new Exception($e->getErrorMessage());
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
        try {
            $list = $this->listContents($dirname, true);
            $objects = [];
            foreach ($list as $val) {
                if ($val['type'] === 'file') {
                    $objects[] = $this->applyPathPrefix($val['path']);
                } else {
                    $objects[] = $this->applyPathPrefix($val['path']) .'/';
                }
            }
            $this->client->deleteObjects($this->bucket, $objects);
            return true;
        } catch (OssException $e) {
            throw new Exception($e->getErrorMessage());
        }
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
        try {
            $object = $this->applyPathPrefix($dirname);
            $options = $this->getOptionsFromConfig($config);
            $this->client->createObjectDir($this->bucket, $object, $options);
            return ['path' => $dirname, 'type' => 'dir'];
        } catch (OssException $e) {
            throw new Exception($e->getErrorMessage());
        }
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
        $objectMeta = $this->getMetadata($path);
        return $objectMeta['info']['url'];
    }

    /**
     * Get a temporary URL for the file at the given path.
     *
     * @param  string  $path
     * @param  \DateTimeInterface  $expiration
     * @param  array  $options
     * @return string
     *
     * @throws \RuntimeException
     */
    public function getTemporaryUrl(string $path, DateTimeInterface $expiration, array $options = []): string
    {
        try {
            $object = $this->applyPathPrefix($path);
            $expires = $expiration->getTimestamp() - time();
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
            $options['headers'][OssClient::OSS_OBJECT_ACL] = $visibility == AdapterInterface::VISIBILITY_PUBLIC ? 'public-read' : 'private';
        }

        foreach (static::$metaOptions as $option) {
            if ($config->has($option)) {
                $options[$option] = $config->get($option);
            }
        }
        return $options;
    }
}