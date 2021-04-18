<?php
declare(strict_types=1);

namespace App\Command;

use Cake\Command\Command;
use Cake\Console\Arguments;
use Cake\Console\ConsoleIo;
use Cake\Console\ConsoleOptionParser;
use Cake\Datasource\ConnectionManager;
use ZipArchive;

/** tmpディレクトリのパス(phpstanでpaths.phpに設定されてるTMPが参照できず、エラー吐かれるので別途定義) */
if (!defined('TMP')) {
    define('TMP', dirname(__DIR__, 2) . DS . 'tmp' . DS);
}

/**
 * ImportDatabase command.
 * sakilaデータベースをCakePHP4で使用するのに良さそうな形に整形する
 */
class ImportDatabaseCommand extends Command
{
    /** ローカルに保存したsakila-db.zipのパス */
    private const SAKILA_ZIP_PATH = TMP . 'sakila-db.zip';
    /** 展開した際のsakila-data.sqlのパス */
    private const SAKILA_DATA_SQL_PATH = TMP . 'sakila-db' . DS . 'sakila-data.sql';
    /** 展開した際のsakila-schema.sqlのパス */
    private const SAKILA_SCHEMA_SQL_PATH = TMP . 'sakila-db' . DS . 'sakila-schema.sql';

    /**
     * Hook method for defining this command's option parser.
     *
     * @see https://book.cakephp.org/4/en/console-commands/commands.html#defining-arguments-and-options
     * @param \Cake\Console\ConsoleOptionParser $parser The parser to be defined
     * @return \Cake\Console\ConsoleOptionParser The built parser.
     */
    public function buildOptionParser(ConsoleOptionParser $parser): ConsoleOptionParser
    {
        $parser = parent::buildOptionParser($parser);

        return $parser;
    }

    /**
     * Implement this method with your command's logic.
     *
     * @param \Cake\Console\Arguments $args The command arguments.
     * @param \Cake\Console\ConsoleIo $io The console io
     * @return null|void|int The exit code or null for success
     */
    public function execute(Arguments $args, ConsoleIo $io)
    {
        // ダウンロード
        if (!file_exists(self::SAKILA_ZIP_PATH)) {
            $io->out('Download sakila-db.zip');
            $file = file_get_contents('http://downloads.mysql.com/docs/sakila-db.zip');
            file_put_contents(self::SAKILA_ZIP_PATH, $file);
        }

        // 展開
        $io->out('Extract sakila-db.zip');
        $zip = new ZipArchive();
        if ($zip->open(self::SAKILA_ZIP_PATH) !== true) {
            $io->abort('sakila-db.zip failed to open.');
        } elseif ($zip->extractTo(TMP) !== true) {
            $io->abort('sakila-db.zip failed to extract.');
        }
        $zip->close();

        // SQLファイルが存在するかチェック
        if (!file_exists(self::SAKILA_DATA_SQL_PATH)) {
            $io->abort('sakila-data.sql not found.');
        } elseif (!file_exists(self::SAKILA_SCHEMA_SQL_PATH)) {
            $io->abort('sakila-schema.sql not found.');
        }

        // MySQLDBにインポート
        $dbConfig = ConnectionManager::getConfig('default');
        $cmd = "mysql -u {$dbConfig['username']} -p{$dbConfig['password']} -P {$dbConfig['port']} ";
        $cmd .= "{$dbConfig['database']} < " . self::SAKILA_SCHEMA_SQL_PATH;
        $io->out('exec ' . $cmd);
        exec($cmd);
        $cmd = "mysql -u {$dbConfig['username']} -p{$dbConfig['password']} -P {$dbConfig['port']} ";
        $cmd .= "{$dbConfig['database']} < " . self::SAKILA_DATA_SQL_PATH;
        $io->out('exec ' . $cmd);
        exec($cmd);

        $conn = ConnectionManager::get('default');

        // ビューとトリガー削除、トリガーによってのみ登録/更新/削除されるfilm_textテーブル削除
        $viewNames = [
            'actor_info',
            'customer_list',
            'film_list',
            'nicer_but_slower_film_list',
            'sales_by_film_category',
            'sales_by_store',
            'staff_list',
        ];
        foreach ($viewNames as $viewName) {
            $query = "DROP VIEW IF EXISTS {$viewName}";
            $io->out($query);
            $conn->execute($query);
        }
        $triggerNames = ['ins_film', 'upd_film', 'del_film', 'rental_date', 'payment_date', 'customer_create_date'];
        foreach ($triggerNames as $triggerName) {
            $query = "DROP TRIGGER IF EXISTS {$triggerName}";
            $io->out($query);
            $conn->execute($query);
        }
        $conn->execute('DROP TABLE IF EXISTS film_text');

        // 外部キー制約をすべて削除
        $foreign_constraints = [
            'address' => ['fk_address_city'],
            'city' => ['fk_city_country'],
            'customer' => ['fk_customer_address', 'fk_customer_store'],
            'film' => ['fk_film_language', 'fk_film_language_original'],
            'film_actor' => ['fk_film_actor_actor', 'fk_film_actor_film'],
            'film_category' => ['fk_film_category_category', 'fk_film_category_film'],
            'inventory' => ['fk_inventory_film', 'fk_inventory_store'],
            'payment' => ['fk_payment_customer', 'fk_payment_rental', 'fk_payment_staff'],
            'rental' => ['fk_rental_customer', 'fk_rental_inventory', 'fk_rental_staff'],
            'staff' => ['fk_staff_address', 'fk_staff_store'],
            'store' => ['fk_store_address', 'fk_store_staff'],
        ];
        foreach ($foreign_constraints as $tableName => $constraints) {
            foreach ($constraints as $constraint) {
                $query = "ALTER TABLE {$tableName} DROP FOREIGN KEY {$constraint}";
                $io->out($query);
                $conn->execute($query);
            }
        }

        // 各テーブルのプライマリキーのカラム名を「id」に変更
        $renameColumns = [
            'actor' => 'actor_id',
            'address' => 'address_id',
            'category' => 'category_id',
            'city' => 'city_id',
            'country' => 'country_id',
            'customer' => 'customer_id',
            'film' => 'film_id',
            'inventory' => 'inventory_id',
            'language' => 'language_id',
            'payment' => 'payment_id',
            'rental' => 'rental_id',
            'staff' => 'staff_id',
            'store' => 'store_id',
        ];
        foreach ($renameColumns as $tableName => $renameColumn) {
            $query = "ALTER TABLE {$tableName} CHANGE {$renameColumn} id int(11) NOT NULL AUTO_INCREMENT";
            $io->out($query);
            $conn->execute($query);
        }

        // 複合プライマリキーを削除
        // 「id」カラムを追加
        // 連番付与
        // プライマリキー設定とオートインクリメント設定
        foreach (['film_actor', 'film_category'] as $tableName) {
            $query = "ALTER TABLE {$tableName} DROP PRIMARY KEY";
            $io->out($query);
            $conn->execute($query);
            $query = "ALTER TABLE {$tableName} ADD COLUMN id int(11) NOT NULL FIRST";
            $io->out($query);
            $conn->execute($query);
            $query = "SET @CNT:=0; UPDATE {$tableName} SET id = (@CNT := @CNT + 1 )";
            $io->out($query);
            $conn->execute($query);
            $query = "ALTER TABLE {$tableName} ADD PRIMARY KEY (id)";
            $io->out($query);
            $conn->execute($query);
            $query = "ALTER TABLE {$tableName} CHANGE id id int(11) NOT NULL AUTO_INCREMENT";
            $io->out($query);
            $conn->execute($query);
        }

        $results = $conn->execute(
            'SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?',
            [
                $dbConfig['database'],
            ]
        )->fetchAll('assoc');
    }
}
