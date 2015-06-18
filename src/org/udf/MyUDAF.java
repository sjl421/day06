package org.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;
//UDAF是输入多个数据行，产生一个数据行
//用户自定义的UDAF必须是继承了UDAF，且内部包含多个实现了exec的静态类

public class MyUDAF extends UDAF {
	public static class MaxiNumberIntUDAFEvaluator implements UDAFEvaluator {
		// 最终结果
		private IntWritable result;

		// 负责初始化计算函数并设置它的内部状态，
		// result是存放最终结果的
		@Override
		public void init() {
			result = null;
		}

		// 每次对一个新值进行聚集计算都会调用iterate方法
		public boolean iterate(IntWritable value) {
			if (value == null)
				return false;
			if (result == null) {
				result = new IntWritable(value.get());
			} else {
				result.set(Math.max(result.get(), value.get()));
			}
			return true;
		}

		// Hive需要部分聚集结果的时候会调用该方法
		// 会返回一个封装了聚集计算当前状态的对象
		public IntWritable terminatePartial() {
			return result;
		}

		// 合并两个部分聚集值会调用这个方法
		public boolean merge(IntWritable other) {
			return iterate(other);
		}

		// Hive需要最终聚集结果时候会调用该方法
		public IntWritable terminate() {
			return result;
		}
	}
}
