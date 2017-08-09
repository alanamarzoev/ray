# 1. have the same get_data function, pass in images and labels into where
# it's trajectory, total_reward... in policy_gradient.py
# 2. In train, have the same main loop, but use the ResNet model instead
# of Agent, and replace run_sgd_minibatch with a call to par_opt.optimize.
# 3. par_opt should be defined in the driver, not the model.
# 4. The model should have a build_loss function (returns costs).

"""ResNet training script, with some code from
https://github.com/tensorflow/models/tree/master/resnet.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from ray.rllib.parallel import LocalSyncParallelOptimizer

import argparse
import os
import numpy as np
import ray
import tensorflow as tf

import cifar_input
import resnet_model_SGD

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
parser.add_argument("--num_gpus", default=0, type=int,
                    help="Number of GPUs to use for training.")

FLAGS = parser.parse_args()

# Determines if the actors require a gpu or not.
use_gpu = 1 if int(FLAGS.num_gpus) > 0 else 0


def get_data(path, size, dataset):
    # Retrieves all preprocessed images and labels using a tensorflow queue.
    # This only uses the cpu.
    os.environ["CUDA_VISIBLE_DEVICES"] = ""
    with tf.device("/cpu:0"):
        queue = cifar_input.build_data(path, size, dataset)
        sess = tf.Session()
        coord = tf.train.Coordinator()
        tf.train.start_queue_runners(sess, coord=coord)
        images, labels = sess.run(queue)
        coord.request_stop()
        sess.close()
        print(type(images), type(labels))
        return images, labels


def compute_steps():
        num_gpus = FLAGS.num_gpus
        ray.init(num_gpus=num_gpus, redirect_output=False)
        train_data = get_data(FLAGS.train_data_path, 50000, FLAGS.dataset)
        num_sgd_iter = 5  # change this
        data = {"images": train_data[0] , "labels": train_data[1]}
        kl_coeff = 0.2
        write_tf_logs = True
        global_step = 0
        j = 0
        dataset = FLAGS.dataset
        hps = resnet_model_SGD.HParams(
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

        images = tf.placeholder(tf.float32, shape=(50000, 32, 32, 3))
        labels = tf.placeholder(tf.float32, shape=(50000, 1))

        model = resnet_model_SGD.ResNet(hps, images, labels, "train")

        if model.hps.optimizer == 'sgd':
            optimizer = tf.train.GradientDescentOptimizer(model.hps.lrn_rate)
        elif model.hps.optimizer == 'mom':
            optimizer = tf.train.MomentumOptimizer(model.hps.lrn_rate, 0.9)

        if model.hps.num_gpus > 0:
            os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(
                [str(i) for i in ray.get_gpu_ids()])
            devices = os.environ["CUDA_VISIBLE_DEVICES"]
        else:
            devices = ["/cpu:0"]

        model.devices = devices
        per_device_batch_size = int(model.hps.batch_size/max(1,num_gpus))

        par_opt = LocalSyncParallelOptimizer(
            optimizer,
            model.devices,
            [images, labels],
            per_device_batch_size,
            model._build_model,
            "~/")

        sess = tf.Session()
        print("HERE")
        tuples_per_device = par_opt.load_data(sess,
                [data["images"], data["labels"]], j == 0 and config["full_trace_data_load"])

        # SGD loop
        for i in range(num_sgd_iter):
            print("HERE2")
            sgd_start = time.time()
            batch_index = 0
            num_batches = (
                int(tuples_per_device) // int(per_device_batch_size))
            loss, kl, entropy = [], [], []
            permutation = np.random.permutation(num_batches)
            while batch_index < num_batches:
                full_trace = (
                    i == 0 and j == 0 and
                    batch_index == config["full_trace_nth_sgd_batch"])
                batch_loss, batch_kl, batch_entropy = model.run_sgd_minibatch(
                    permutation[batch_index] * model.per_device_batch_size,
                    kl_coeff, full_trace,
                    file_writer if write_tf_logs else None)
                loss.append(batch_loss)
                kl.append(batch_kl)
                entropy.append(batch_entropy)
                batch_index += 1
            loss = np.mean(loss)
            kl = np.mean(kl)
            entropy = np.mean(entropy)
            sgd_end = time.time()
            print(
                "{:>15}{:15.5e}{:15.5e}{:15.5e}".format(i, loss, kl, entropy))

            values = []
            if i == config["num_sgd_iter"] - 1:
                metric_prefix = "policy_gradient/sgd/final_iter/"
                values.append(tf.Summary.Value(
                    tag=metric_prefix + "kl_coeff",
                    simple_value=kl_coeff))
            else:
                metric_prefix = "policy_gradient/sgd/intermediate_iters/"
            values.extend([
                tf.Summary.Value(
                    tag=metric_prefix + "mean_entropy",
                    simple_value=entropy),
                tf.Summary.Value(
                    tag=metric_prefix + "mean_loss",
                    simple_value=loss),
                tf.Summary.Value(
                    tag=metric_prefix + "mean_kl",
                    simple_value=kl)])
            if write_tf_logs:
                sgd_stats = tf.Summary(value=values)
                file_writer.add_summary(sgd_stats, global_step)
            global_step += 1
            sgd_time += sgd_end - sgd_start
