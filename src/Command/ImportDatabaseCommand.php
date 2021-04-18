<?php
declare(strict_types=1);

namespace App\Command;

use Cake\Command\Command;
use Cake\Console\Arguments;
use Cake\Console\ConsoleIo;
use Cake\Console\ConsoleOptionParser;
use Cake\Datasource\ConnectionManager;
use ZipArchive;

/**
 * ImportDatabase command.
 * sakilaデータベースをCakePHP4で使用するのに良さそうな形に整形する
 */
class ImportDatabaseCommand extends Command
{
    /** ローカルに保存したsakila-db.zipのパス */
    const SAKILA_ZIP_PATH = TMP . 'sakila-db.zip';
    /** 展開した際のsakila-data.sqlのパス */
    const SAKILA_DATA_SQL_PATH = TMP . 'sakila-db' . DS . 'sakila-data.sql';
    /** 展開した際のsakila-schema.sqlのパス */
    const SAKILA_SCHEMA_SQL_PATH = TMP . 'sakila-db' . DS . 'sakila-schema.sql';

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
        $db_config = ConnectionManager::getConfig('default');
        $cmd = "mysql -u {$db_config['username']} -p{$db_config['password']} -P {$db_config['port']} {$db_config['database']} < " . self::SAKILA_SCHEMA_SQL_PATH;
        $io->out('exec ' . $cmd);
        exec($cmd);
        $cmd = "mysql -u {$db_config['username']} -p{$db_config['password']} -P {$db_config['port']} {$db_config['database']} < " . self::SAKILA_DATA_SQL_PATH;
        $io->out('exec ' . $cmd);
        exec($cmd);

        $conn = ConnectionManager::get('default');

        // ビューとトリガー削除、トリガーによってのみ登録/更新/削除されるfilm_textテーブル削除
        $view_names = ['actor_info', 'customer_list', 'film_list', 'nicer_but_slower_film_list', 'sales_by_film_category', 'sales_by_store', 'staff_list'];
        foreach ($view_names as $view_name) {
            $query = "DROP VIEW IF EXISTS {$view_name}";
            $io->out($query);
            $conn->execute($query);
        }
        $trigger_names = ['ins_film', 'upd_film', 'del_film', 'rental_date', 'payment_date', 'customer_create_date'];
        foreach ($trigger_names as $trigger_name) {
            $query = "DROP TRIGGER IF EXISTS {$trigger_name}";
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
        foreach ($foreign_constraints as $table_name => $constraints) {
            foreach ($constraints as $constraint) {
                $query = "ALTER TABLE {$table_name} DROP FOREIGN KEY {$constraint}";
                $io->out($query);
                $conn->execute($query);
            }
        }

        // 各テーブルのプライマリキーのカラム名を「id」に変更
        $rename_columns = [
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
        foreach ($rename_columns as $table_name => $rename_column) {
            $query = "ALTER TABLE {$table_name} CHANGE {$rename_column} id int(11) NOT NULL AUTO_INCREMENT";
            $io->out($query);
            $conn->execute($query);
        }

        // 複合プライマリキーを削除
        // 「id」カラムを追加
        // 連番付与
        // プライマリキー設定とオートインクリメント設定
        foreach (['film_actor', 'film_category'] as $table_name) {
            $query = "ALTER TABLE {$table_name} DROP PRIMARY KEY";
            $io->out($query);
            $conn->execute($query);
            $query = "ALTER TABLE {$table_name} ADD COLUMN id int(11) NOT NULL FIRST";
            $io->out($query);
            $conn->execute($query);
            $query = "SET @CNT:=0; UPDATE {$table_name} SET id = (@CNT := @CNT + 1 )";
            $io->out($query);
            $conn->execute($query);
            $query = "ALTER TABLE {$table_name} ADD PRIMARY KEY (id)";
            $io->out($query);
            $conn->execute($query);
            $query = "ALTER TABLE {$table_name} CHANGE id id int(11) NOT NULL AUTO_INCREMENT";
            $io->out($query);
            $conn->execute($query);
        }

        $results = $conn->execute('SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?', [$db_config['database']])->fetchAll('assoc');
        debug($results);
    }
}