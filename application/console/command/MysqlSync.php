<?php
/**
 * Class MysqlSync
 *
 * PHP version 5.x
 *
 * @category MysqlSync
 * @package  App
 * @author   Richie <richebossman@163.com>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License
 * @link     http://www.uusense.com
 */
namespace app\console\command;

use think\console\Command;
use think\console\Input;
use think\console\input\Option;
use think\console\Output;
use think\Db;

/**
 * Class MysqlSync
 *
 * @category MysqlSync
 * @package  App
 * @author   Richie <richebossman@163.com>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License
 * @link     http://www.uusense.com
 */
class MysqlSync extends Command
{
    protected $input;

    protected $output;

    protected $dsn;

    protected $master;

    protected $slave;

    protected $runtimeTableName = 'mysql_sync_runtime';

    protected $limit = 5000;

    /**
     * 配置
     *
     * @return bool
     */
    protected function configure()
    {
        // 指令配置
        $this
            ->setName('mysql-sync')
            ->addOption('master', null, Option::VALUE_REQUIRED, 'mysql://username:password@host:port/database?')
            ->addOption('slave', null, Option::VALUE_REQUIRED, 'mysql://username:password@host:port/database?')
            ->addOption('limit', null, Option::VALUE_REQUIRED, 'limit number, default:1000')
            ->setDescription('MySQL Databases Sync.');
        return;
    }

    /**
     * 执行入口
     *
     * @param Input  $input  输入
     * @param Output $output 输出
     *
     * @return int|null|void
     */
    protected function execute(Input $input, Output $output)
    {
        $this->input = $input;
        $this->output = $output;

        $this
            ->checkDatabaseInfo()
            ->checkConnection()
            ->checkRuntimeTable()
            ->runTask();

        $this->info(str_repeat('=', 50));
    }

    /**
     * 检查主从数据库输入参数
     *
     * @return $this
     */
    protected function checkDatabaseInfo()
    {
        $this->info('Check database info.');
        $master = $this->input->getOption('master');
        if (!$master) {
            $this->error('master option is error.');
        }
        $slave = $this->input->getOption('slave');
        if (!$slave) {
            $this->error('slave option is error.');
        }
        $master = $this->parseDsn($master);
        $slave = $this->parseDsn($slave);

        if (!$master['username'] || !$master['hostname'] || !$master['hostport'] || !$master['database']) {
            $this->error('master database info is error. use: mysql://username:password@host:port/database');
        }
        if (!$slave['username'] || !$slave['hostname'] || !$slave['hostport'] || !$slave['database']) {
            $this->error('slave database info is error. use: mysql://username:password@host:port/database');
        }
        $this->dsn = ['master' => $master, 'slave' => $slave];
        return $this;
    }

    /**
     * 检查主从数据库的连接
     *
     * @return $this
     */
    protected function checkConnection()
    {
        $this->info('Check database connection.');
        $master = array_merge(config('database'), $this->dsn['master']);
        $this->master = Db::connect($master, true);
        $data = $this->master->query('SHOW TABLES');
        $this->info('Master database: ' . count($data) . ' tables.');
        $slave = array_merge(config('database'), $this->dsn['slave']);
        $this->slave = Db::connect($slave, true);
        $data = $this->slave->query('SHOW TABLES');
        $this->info('Slave database: ' . count($data) . ' tables.');
        return $this;
    }

    /**
     * 检查运行时表
     *
     * @return $this
     */
    protected function checkRuntimeTable()
    {
        $this->info('Check runtime table.');
        $this->slave->execute($this->getRuntimeTableSql());
        return $this;
    }

    /**
     * 执行同步任务
     *
     * @return bool
     */
    protected function runTask()
    {
        $this->info('Get table status');
        $sql = 'SHOW TABLE STATUS';
        $masterTables = $this->parseTableInfo($this->master->query($sql));
        $slaveTables = $this->parseTableInfo($this->slave->query($sql));
        $this->info('Run tables.');
        foreach ($masterTables as $masterTable) {
            if (!$masterTable['Engine']) {
                $this->info($masterTable['Name'] . ' no Engine.');
                continue;
            }
            $this->info(str_repeat('-', 50));
            // not create table.
            if (!isset($slaveTables[$masterTable['Name']]) && !$this->checkTableExists($masterTable['Name'])) {
                $this->info('Create table ' . $masterTable['Name']);
                $tableSql = $this->getCreateTableSql($masterTable['Name']);
                $pkId = $this->master->getTableInfo($masterTable['Name'], 'pk');
                if (is_array($pkId)) {
                    $pkId = $pkId[0];
                }
                $this->slave->execute($tableSql);
                $this->slave
                    ->table($this->runtimeTableName)
                    ->insert([
                        'tableName' => $masterTable['Name'],
                        'addDate' => date('Y-m-d H:i:s', time()),
                        'lastTime' => date('Y-m-d H:i:s', time()),
                        'lastPkId' => 0,
                        'pkId' => $pkId,
                        'rows' => 0
                    ]);
                $this->info('Added table ' . $masterTable['Name']);
            } else {
                $this->info('Run Table ' . $masterTable['Name']);
                $this->info('Get runtime info.');
                $runtime = $this->slave
                    ->table($this->runtimeTableName)
                    ->where(['tableName' => $masterTable['Name']])
                    ->find();
                $this->info('Get table pk.');
                $pk = $runtime['pkId'] ? $runtime['pkId'] : $pkId = $this->master->getTableInfo($masterTable['Name'], 'pk');
                if (is_array($pk)) {
                    $pk = $pk[0];
                }
                $this->info('PK is ' . $pk);
                $this->info('Get Max PkId');
                $maxPkId = $this->master->table($masterTable['Name'])->max($pk);
                $this->info('Max PkId is ' . $maxPkId);
                if ($maxPkId <= $runtime['pkId']) {
                    $this->info('Skip Table ' . $masterTable['Name']);
                    continue;
                }
                $this->info('Get Master table data.');
                $data = $this->master
                    ->table($masterTable['Name'])
                    ->order($pk . ' ASC')
                    ->limit($this->limit)
                    ->where([$pk => ['gt', $runtime['lastPkId']]])
                    ->select();
                if ($data) {
                    $this->info('Insert into slave data.');
                    $end = end($data);
                    $lastPkId = $end[$pk];
                    $this->info('lastPkId value ' . $lastPkId);
                    $this->slave
                        ->table($masterTable['Name'])
                        ->insertAll($data);
                    $this->info('Update runtime info.');
                    $this->slave
                        ->table($this->runtimeTableName)
                        ->where('tableName', $masterTable['Name'])
                        ->update(['lastPkId' => $lastPkId, 'lastTime' => date('Y-m-d H:i:s', time()), 'rows' => $runtime['rows'] + count($data)]);
                }
                $this->info('Table ' . $masterTable['Name'] . ' Added ' . count($data));
            }
        }
        $this->info('This task ok.');
        return;
    }

    /**
     * 格式化表信息数据
     *
     * @param array $tables 表数据
     *
     * @return array
     */
    protected function parseTableInfo($tables = [])
    {
        $table = [];
        foreach ($tables as $tab) {
            $table[$tab['Name']] = $tab;
        }
        return $table;
    }

    /**
     * 检查从库表是否存在
     *
     * @param string $table 表名
     *
     * @return bool
     */
    protected function checkTableExists($table = '')
    {
        $res = $this->slave->query('SHOW TABLES LIKE "' . $table . '"');
        if (count($res) > 0) {
            return true;
        }
        return false;
    }

    /**
     * 获取创建表的SQL
     *
     * @param string $table 表名
     * @return mixed
     */
    protected function getCreateTableSql($table = '')
    {
        $createSql = $this->master->query('SHOW CREATE TABLE `' . $table . '`');
        $createSql = str_replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS', $createSql[0]['Create Table']);
        return $createSql;
    }

    /**
     * 连接数据库的dsn信息
     *
     * @param string $dsnStr dsn信息
     *
     * @return array
     */
    protected function parseDsn($dsnStr = '')
    {
        $info = parse_url($dsnStr);
        if (!$info) {
            return [];
        }
        $dsn = [
            'type' => $info['scheme'],
            'username' => isset($info['user']) ? $info['user'] : '',
            'password' => isset($info['pass']) ? $info['pass'] : '',
            'hostname' => isset($info['host']) ? $info['host'] : '',
            'hostport' => isset($info['port']) ? trim($info['port']) : '3306',
            'database' => !empty($info['path']) ? ltrim($info['path'], '/') : '',
            'charset' => isset($info['fragment']) ? $info['fragment'] : 'utf8',
        ];

        if (isset($info['query'])) {
            parse_str($info['query'], $dsn['params']);
        } else {
            $dsn['params'] = [];
        }
        return $dsn;
    }

    /**
     * 运行时表格SQL
     *
     * @return string
     */
    protected function getRuntimeTableSql()
    {
        return "CREATE TABLE IF NOT EXISTS `" . $this->runtimeTableName . "` (
                  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                  `tableName` varchar(50) NOT NULL DEFAULT '',
                  `addDate` datetime NOT NULL,
                  `lastTime` datetime NOT NULL,
                  `pkId` varchar(50) NOT NULL DEFAULT '',
                  `lastPkId` int(11) NOT NULL,
                  `rows` int(11) NOT NULL,
                  PRIMARY KEY (`id`),
                  KEY `tableName` (`tableName`)
                ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
    }

    /**
     * 错误输出
     *
     * @param string $message 消息
     */
    protected function error($message = '')
    {
        $this->output->writeln('<error>' . $message . '</error>');
        exit;
    }

    /**
     * 消息输出
     *
     * @param string $message 消息
     */
    protected function info($message = '')
    {
        $this->output->writeln('<info>' . $message . '</info>');
    }
}
