/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.chinaunicom.datalabs.mdm.hadoop;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JavaKMeansSuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    SparkConf conf=new SparkConf();
//    conf.set("Master","spark://namenode1:7077");
    sc = new JavaSparkContext("spark://namenode1:7077", "JavaKMeans");

  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @Test
  public void runKMeansUsingStaticMethods() {
    List<Vector> points = Lists.newArrayList(
            Vectors.dense(1.0, 2.0, 6.0),
            Vectors.dense(1.0, 3.0, 0.0),
            Vectors.dense(1.0, 4.0, 6.0)
    );

    Vector expectedCenter = Vectors.dense(1.0, 3.0, 4.0);

    JavaRDD<Vector> data = sc.parallelize(points, 2);
    KMeansModel model = KMeans.train(data.rdd(), 1, 1, 1, KMeans.K_MEANS_PARALLEL());
    assertEquals(1, model.clusterCenters().length);
    assertEquals(expectedCenter, model.clusterCenters()[0]);

    model = KMeans.train(data.rdd(), 1, 1, 1, KMeans.RANDOM());
    assertEquals(expectedCenter, model.clusterCenters()[0]);
  }

  @Test
  public void runKMeansUsingConstructor() {
    List<Vector> points = Lists.newArrayList(
            Vectors.dense(1.0, 2.0, 6.0),
            Vectors.dense(1.0, 3.0, 0.0),
            Vectors.dense(1.0, 4.0, 6.0)
    );

    Vector expectedCenter = Vectors.dense(1.0, 3.0, 4.0);

    JavaRDD<Vector> data = sc.parallelize(points, 2);
    KMeansModel model = new KMeans().setK(1).setMaxIterations(5).run(data.rdd());
    assertEquals(1, model.clusterCenters().length);
    assertEquals(expectedCenter, model.clusterCenters()[0]);

    model = new KMeans()
      .setK(1)
      .setMaxIterations(1)
      .setInitializationMode(KMeans.RANDOM())
      .run(data.rdd());
    assertEquals(expectedCenter, model.clusterCenters()[0]);
  }

  @Test
  public void testPredictJavaRDD() {
    List<Vector> points = Lists.newArrayList(
            Vectors.dense(1.0, 2.0, 6.0),
            Vectors.dense(1.0, 3.0, 0.0),
            Vectors.dense(1.0, 4.0, 6.0)
    );
    JavaRDD<Vector> data = sc.parallelize(points, 2);
    KMeansModel model = new KMeans().setK(1).setMaxIterations(5).run(data.rdd());
    JavaRDD<Integer> predictions = model.predict(data);
    // Should be able to get the first prediction.
    predictions.first();
  }
}