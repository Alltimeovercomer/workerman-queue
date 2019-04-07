<?php
/**
 * This file is part of workerman.
 *
 * Licensed under The MIT License
 * For full copyright and license information, please see the MIT-LICENSE.txt
 * Redistributions of files must retain the above copyright notice.
 *
 * @author walkor<walkor@workerman.net>
 * @copyright walkor<walkor@workerman.net>
 * @link http://www.workerman.net/
 * @license http://www.opensource.org/licenses/mit-license.php MIT License
 */
use Workerman\Worker;
// 自动加载类
// Automatic loading class
require_once __DIR__ . '/../../Workerman/Autoloader.php';


/*******************************************************************
 * 基于Worker实现的一个简单的消息队列服务
 * 服务分为两组进程，
 * 一组监听端口并把发来的数据放到sysv消息队列中
 * 另外一组进程为消费者，负责从队列中读取数据并处理
 * 
 * 注意：
 * 使用的是系统自带的 sysv 队列，即使队列服务重启数据也不会丢失
 * 但服务器重启后数据会丢失
 * 系统默认sysv队列容量比较小，可以根据需要配置Linux内核参数，
 * 增大队列容量
 
 * A simple message queue service based on Worker implementation twenty two  
 * Service is divided into two groups of processes, twenty three  
 * A set of listening ports and put the sent data into the sysv message queue twenty four  
 * Another set of processes is the consumer, responsible for reading data from the queue and processing 25  
 * 26  * Note: 27  
 * The sysv queue that comes with the system is used, even if the queue service restarts the data, it will not be lost. 28  
 * Data will be lost after server restart 29  
 * The system default sysv queue capacity is relatively small, you can configure Linux kernel parameters as needed. 30  
 * Increase queue capacity
 *******************************************************************/

// 队列的id。为了避免混淆，可以和监听的端口相同
// The id of the queue. To avoid confusion, it can be the same as the listening port.
$QUEUE_ID = 1236;

// #######消息队列服务监听的端口##########
// #######Message queue service listening port##########
$msg_recver = new Worker('Text://0.0.0.0:1236');
// 向哪个队列放数据
// To which queue to put data
$msg_recver->queueId = $QUEUE_ID;

if(!extension_loaded('sysvmsg'))
{
    echo "Please install sysvmsg extension.\n";
    exit;
}

/**
 * 进程启动时，初始化sysv消息队列
 * Initialize the sysv message queue when the process starts
 */
$msg_recver->onWorkerStart = function($msg_recver)
{
    $msg_recver->queue = msg_get_queue($msg_recver->queueId);
};

/**
 * 服务接收到消息时，将消息写入系统的sysv消息队列，消费者从该队列中读取
 * When the service receives the message, it writes the message to the system's sysv message queue, and the consumer reads from the queue.
 */
$msg_recver->onMessage = function($connection, $message) use ($msg_recver)
{
    $msgtype = 1;
    $errorcode = 500;
    // @see http://php.net/manual/zh/function.msg-send.php
    if(extension_loaded('sysvmsg') && msg_send( $msg_recver->queue , $msgtype , $message, true , true , $errorcode))
    {
        return $connection->send('{"code":0, "msg":"success"}');
    }
    else 
    {
        return $connection->send('{"code":'.$errorcode.', "msg":"fail"}');
    }
};


// ######## 消息队列消费者 ########
// ######## Message Queue Consumer ########
$consumer = new Worker();
// 消费的队列的id
// The id of the consumed queue
$consumer->queueId = $QUEUE_ID;
// 慢任务，消费者的进程数可以开多一些
// Slow tasks, the number of consumers can open more
$consumer->count = 32;

/**
 * 进程启动阻塞式的从队列中读取数据并处理
 * The process starts blocking and reads data from the queue and processes it
 */
$consumer->onWorkerStart = function($consumer)
{
    // 获得队列资源
    // Get queue resources
    $consumer->queue = msg_get_queue($consumer->queueId);
    \Workerman\Lib\Timer::add(0.5, function() use ($consumer){
        if(extension_loaded('sysvmsg'))
        {
            // 循环取数据
            // Loop data
            while(1)
            {
                $desiredmsgtype = 1;
                $msgtype = 0;
                $message = '';
                $maxsize = 65535;
                // 从队列中获取消息 (Get messages from the queue) @see http://php.net/manual/zh/function.msg-receive.php
                @msg_receive($consumer->queue , $desiredmsgtype , $msgtype , $maxsize , $message, true, MSG_IPC_NOWAIT);
                if(!$message)
                {
                    return;
                }
                // 假设消息数据为json，格式类似{"class":"class_name", "method":"method_name", "args":[]}
                // Suppose the message data is json, the format is similar {"class":"class_name", "method":"method_name", "args":[]}
                $message = json_decode($message, true);
                // 格式如果是正确的，则尝试执行对应的类方法
                // If the format is correct, try to execute the corresponding class method
                if(isset($message['class']) && isset($message['method']) && isset($message['args']))
                {
                    // 要调用的类名，加上Consumer命名空间
                    // The class name to be called, plus the Consumer namespace
                    $class_name = "\\Consumer\\".$message['class'];
                    // 要调用的方法名
                    // The name of the method to call
                    $method = $message['method'];
                    // 调用参数，是个数组
                    // Call the argument, is an array
                    $args = (array)$message['args'];
                     
                    // 类存在则尝试执行
                    // Try to execute if the class exists
                    if(class_exists($class_name))
                    {
                        $class = new $class_name;
                        $callback = array($class, $method);
                        if(is_callable($callback))
                        {
                            call_user_func_array($callback, $args);
                        }
                        else
                        {
                            echo "$class_name::$method not exist\n";
                        }
                    }
                    else
                    {
                        echo "$class_name not exist\n";
                    }
                }
                else 
                {
                    echo "unknow message\n";
                }
            }
        }
    });
};

// 如果不是在根目录启动，则运行runAll方法
// Run the runAll method if it is not started in the root directory
if(!defined('GLOBAL_START'))
{
    Worker::runAll();
}
