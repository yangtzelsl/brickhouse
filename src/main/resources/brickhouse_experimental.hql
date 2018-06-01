
CREATE TEMPORARY FUNCTION vector_op AS 'brickhouse.experimental.vectorVectorOpUDF';
CREATE TEMPORARY FUNCTION vector_agg_op AS 'brickhouse.experimental.vector.VectorAggOpUDF';
CREATE TEMPORARY FUNCTION vector_collector_op AS 'brickhouse.experimental.vector.VectorCollectorOpUDAF';
CREATE TEMPORARY FUNCTION vector_collector_op_aprox AS 'brickhouse.experimental.vector.VectorCollectorOpAproxUDAF';
CREATE TEMPORARY FUNCTION vector_rollup AS 'brickhouse.experimental.vector.VectorRollUpUDF';
CREATE TEMPORARY FUNCTION vector_kernel_factory AS 'brickhouse.experimental.vector.VectorKernelFactoryUDF';