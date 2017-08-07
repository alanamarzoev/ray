"""ResNet training script, with some code from
https://github.com/tensorflow/models/tree/master/resnet.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os
import numpy as np
import ray
import tensorflow as tf

import cifar_input
import resnet_model_multi

# Tensorflow must be at least version 1.0.0 for the example to work.
if int(tf.__version__.split(".")[0]) < 1:
    raise Exception("Your Tensorflow version is less than 1.0.0. Please "
                    "update Tensorflow to the latest version.")

parser = argparse.ArgumentParser(description="Run the ResNet example.")
parser.add_argument("--dataset", default="cifar10", type=str,
                    help="Dataset to use: cifar10 or cifar100.")
parser.add_argument("--train_data_path",
                    default="cifar-10-batches-bin/data_batch*", type=str,
                    help="Data path for the training data.")
parser.add_argument("--eval_data_path",
                    default="cifar-10-batches-bin/test_batch.bin", type=str,
                    help="Data path for the testing data.")
parser.add_argument("--eval_dir", default="/tmp/resnet-model/eval", type=str,
                    help="Data path for the tensorboard logs.")
parser.add_argument("--eval_batch_count", default=50, type=int,
                    help="Number of batches to evaluate over.")
parser.add_argument("--num_gpus", default=8, type=int,
                    help="Number of GPUs to use for training.")

FLAGS = parser.parse_args()

# Determines if the actors require a gpu or not.
use_gpu = 8 if int(FLAGS.num_gpus) > 0 else 8


@ray.remote
def get_data(path, size, dataset):
    # Retrieves all preprocessed images and labels using a tensorflow queue.
    # This only uses the cpu.
    # os.environ["CUDA_VISIBLE_DEVICES"] = ""
    with tf.device("/cpu:0"):
        queue = cifar_input.build_data(path, size, dataset)
        sess = tf.Session()
        coord = tf.train.Coordinator()
        tf.train.start_queue_runners(sess, coord=coord)
        images, labels = sess.run(queue)
        print("HERE1")
        print((images.shape, labels.shape))
        coord.request_stop()
        sess.close()
        return images, labels


@ray.remote(num_gpus=use_gpu)
class ResNetTrainActor(object):
    def __init__(self, data, dataset, num_gpus):
        if num_gpus > 0:
            os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(
                [str(i) for i in ray.get_gpu_ids()])
        print("HI THE NUM OF GPUS IS " + str(num_gpus))
        hps = resnet_model_multi.HParams(
            batch_size=100,
            num_classes=100 if dataset == "cifar100" else 10,
            min_lrn_rate=0.0001,
            lrn_rate=0.1,
            num_residual_units=5,
            use_bottleneck=False,
            weight_decay_rate=0.0002,
            relu_leakiness=0.1,
            optimizer="mom",
            num_gpus=num_gpus)

        # We seed each actor differently so that each actor operates on a
        # different subset of data.
        # if num_gpus > 0:
        #     tf.set_random_seed(ray.get_gpu_ids()[0] + 1)
        # else:
        #     # Only a single actor in this case.
        #     tf.set_random_seed(1)

        input_images = data[0]
        input_labels = data[1]
        print(input_images.shape)
        with tf.device("/gpu:0" if num_gpus > 0 else "/cpu:0"):
            # Build the model.
            images, labels = cifar_input.build_input([input_images,
                                                      input_labels],
                                                     hps.batch_size, dataset,
                                                     False)
            self.model = resnet_model_multi.ResNet(hps, images, labels, "train")
            self.model.build_graph()
            config = tf.ConfigProto(allow_soft_placement=True)
            sess = tf.Session(config=config)
            self.model.variables.set_session(sess)
            self.model.par_opt.load_data(sess, images)
            self.coord = tf.train.Coordinator()
            tf.train.start_queue_runners(sess, coord=self.coord)
            init = tf.global_variables_initializer()
            sess.run(init)
            self.steps = 10

    def compute_steps(self, weights):
        # This method sets the weights in the network, trains the network
        # self.steps times, and returns the new weights.
        self.model.variables.set_weights(weights)
        for i in range(self.steps):
            self.par_opt.optimize(self.model.variables.sess, i,
                                  extra_ops=self.model.train_op)
            # self.model.variables.sess.run(self.model.train_op)
        return self.model.variables.get_weights()

    def get_weights(self):
        # Note that the driver cannot directly access fields of the class,
        # so helper methods must be created.
        return self.model.variables.get_weights()


@ray.remote
class ResNetTestActor(object):
    def __init__(self, data, dataset, eval_batch_count, eval_dir):
        hps = resnet_model_multi.HParams(
            batch_size=100,
            num_classes=100 if dataset == "cifar100" else 10,
            min_lrn_rate=0.0001,
            lrn_rate=0.1,
            num_residual_units=5,
            use_bottleneck=False,
            weight_decay_rate=0.0002,
            relu_leakiness=0.1,
            optimizer="mom",
            num_gpus=1)
        input_images = data[0]
        input_labels = data[1]
        with tf.device("/cpu:0"):
            # Builds the testing network.
            images, labels = cifar_input.build_input([input_images,
                                                      input_labels],
                                                     hps.batch_size, dataset,
                                                     False)
            self.model = resnet_model_multi.ResNet(hps, images, labels, "eval")
            self.model.build_graph()
            config = tf.ConfigProto(allow_soft_placement=True)
            sess = tf.Session(config=config)
            self.model.variables.set_session(sess)
            self.coord = tf.train.Coordinator()
            tf.train.start_queue_runners(sess, coord=self.coord)
            init = tf.global_variables_initializer()
            sess.run(init)

            # Initializing parameters for tensorboard.
            self.best_precision = 0.0
            self.eval_batch_count = eval_batch_count
            self.summary_writer = tf.summary.FileWriter(eval_dir, sess.graph)
        # The IP address where tensorboard logs will be on.
        self.ip_addr = ray.services.get_node_ip_address()

    def accuracy(self, weights, train_step):
        # Sets the weights, computes the accuracy and other metrics
        # over eval_batches, and outputs to tensorboard.
        self.model.variables.set_weights(weights)
        total_prediction, correct_prediction = 0, 0
        model = self.model
        sess = self.model.variables.sess
        for _ in range(self.eval_batch_count):
            summaries, loss, predictions, truth = sess.run(
                [model.summaries, model.cost, model.predictions,
                 model.labels])

            truth = np.argmax(truth, axis=1)
            predictions = np.argmax(predictions, axis=1)
            correct_prediction += np.sum(truth == predictions)
            total_prediction += predictions.shape[0]

        precision = 1.0 * correct_prediction / total_prediction
        self.best_precision = max(precision, self.best_precision)
        precision_summ = tf.Summary()
        precision_summ.value.add(
            tag="Precision", simple_value=precision)
        self.summary_writer.add_summary(precision_summ, train_step)
        best_precision_summ = tf.Summary()
        best_precision_summ.value.add(
            tag="Best Precision", simple_value=self.best_precision)
        self.summary_writer.add_summary(best_precision_summ, train_step)
        self.summary_writer.add_summary(summaries, train_step)
        tf.logging.info("loss: %.3f, precision: %.3f, best precision: %.3f" %
                        (loss, precision, self.best_precision))
        self.summary_writer.flush()
        return precision

    def get_ip_addr(self):
        # As above, a helper method must be created to access the field from
        # the driver.
        return self.ip_addr


def train():
    print("hi")
    num_gpus = FLAGS.num_gpus
    ray.init(num_gpus=num_gpus, redirect_output=False)
    num_machines = len(ray.global_state.client_table().keys())
    train_data = get_data.remote(FLAGS.train_data_path, 50000, FLAGS.dataset)
    print(train_data)
    test_data = get_data.remote(FLAGS.eval_data_path, 10000, FLAGS.dataset)

    # import IPython
    # IPython.embed()
    #print(list(x.shape for x in test_data))
    # Creates an actor for each machine. Each actor has access to the dataset.
    if FLAGS.num_gpus > 0:
        print("SUPPPP" + str(FLAGS.num_gpus))
        num_gpus = FLAGS.num_gpus
        train_actors = [ResNetTrainActor.remote(train_data, FLAGS.dataset,
                                                num_gpus)
                        for _ in range(num_machines)]
    else:
        train_actors = [ResNetTrainActor.remote(train_data, FLAGS.dataset, 0)]

    test_actor = ResNetTestActor.remote(test_data, FLAGS.dataset,
                                        FLAGS.eval_batch_count, FLAGS.eval_dir)
    print("The log files for tensorboard are stored at ip {}."
          .format(ray.get(test_actor.get_ip_addr.remote())))
    step = 0
    weight_id = train_actors[0].get_weights.remote()
    acc_id = test_actor.accuracy.remote(weight_id, step)
    # Correction for dividing the weights by the number of gpus.
    if num_gpus == 0:
        num_gpus = 1
    print("Starting training loop. Use Ctrl-C to exit.")
    try:
        while True:
            all_weights = ray.get([actor.compute_steps.remote(weight_id)
                                   for actor in train_actors])
            # mean_weights = {k: (sum([weights[k] for weights in all_weights]) /
            #                     num_machines)
            #                 for k in all_weights[0]}
            # weight_id = ray.put(mean_weights)
            # step += 10
    #         if step % 200 == 0:
    #             # Retrieves the previously computed accuracy and launches a new
    #             # testing task with the current weights every 200 steps.
    #             acc = ray.get(acc_id)
    #             acc_id = test_actor.accuracy.remote(weight_id, step)
    #             print("Step {0}: {1:.6f}".format(step - 200, acc))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    train()
