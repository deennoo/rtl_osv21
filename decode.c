#include "Python.h"
#include <math.h>
#include <stdio.h>
#include <stdlib.h>


#define SMOOTH_WINDOW 488


static PyObject *readFile(PyObject *self, PyObject *args) {
	PyObject *ph, *output, *bits, *temp;
	int i, j;
	
	if(!PyArg_ParseTuple(args, "O", &ph)) {
		PyErr_Format(PyExc_RuntimeError, "Invalid parameters");
		return NULL;
	}
	
	// Ready the file
	FILE *fh = PyFile_AsFile(ph);
	PyFile_IncUseCount((PyFileObject *) ph);
	
	// Setup the output list
	bits = PyList_New(0);
	
	// Setup the variables
	float threshold = 6800.0;
	float real, imag;
	float instPower;
	float runningSum = 0;
	float *buffer;
	buffer = (float *) malloc(SMOOTH_WINDOW*sizeof(float));
	for(i=0; i<SMOOTH_WINDOW; i++) {
		*(buffer + i) = 0.0;
	}
	
	// Setup the control loop
	int power, edge;
	int prevPower = 0;
	long dataCounter = 0;
	long prevEdge = -1;
	long edgeCountDiff = -1;
	long halfTime = 0;
	int addBit;
	
	unsigned char raw[2*SMOOTH_WINDOW];
	i = fread(raw, 1, sizeof(raw), fh);
	while( !feof(fh) ) {
		for(j=0; j<SMOOTH_WINDOW; j++) {
			real = ((float) raw[2*j+0]) - 127.0;
			imag = ((float) raw[2*j+1]) - 127.0;
			instPower = real*real + imag*imag;
			dataCounter += 1;
		
			// Moving average
			runningSum += instPower - *(buffer + j);
			*(buffer + j) = instPower;
		
			// Convert to an integer
			if( runningSum >= threshold*SMOOTH_WINDOW ) {
				power = 1;
			} else {
				power = 0;
			}
		
			// Edge detection
			edge = power - prevPower;
			prevPower = power;
		
			// Timing
			if( edge != 0 ) {
				if( prevEdge < 0 ) {
					prevEdge = dataCounter;
				}
				edgeCountDiff = dataCounter - prevEdge;
			}
				
			if( edge == 1 ) {
				// Rising edge
		
				if( edgeCountDiff > 80000 ) {
					prevEdge = dataCounter;
					halfTime = 0;
					addBit = 1;
				} else if( edgeCountDiff < 200 || edgeCountDiff > 1100 ) {
					addBit = 0;
				} else if( edgeCountDiff < 615 ) {
					prevEdge = dataCounter;
					halfTime += 1;
					addBit = 1;
				} else {
					prevEdge = dataCounter;
					halfTime += 2;
					addBit = 1;
				}
			
				if( addBit && halfTime % 2 == 0 ) {
					temp = PyInt_FromLong(1);
					PyList_Append(bits, temp);
					Py_DECREF(temp);
				}
			
			} else if( edge == -1 ) {
				// Falling edge
			
				if( edgeCountDiff > 80000 ) {
					prevEdge = dataCounter;
					halfTime = 0;
					addBit = 1;
				} else if( edgeCountDiff < 400 || edgeCountDiff > 1400 ) {
					addBit = 0;
				} else if( edgeCountDiff < 850 ) {
					prevEdge = dataCounter;
					halfTime += 1;
					addBit = 1;
				} else {
					prevEdge = dataCounter;
					halfTime += 2;
					addBit = 1;
				}
			
				if( addBit && halfTime % 2 == 0 ) {
					temp = PyInt_FromLong(0);
					PyList_Append(bits, temp);
					Py_DECREF(temp);
				}
			}
		}
		
		// Read in the next sample
		i = fread(raw, 1, sizeof(raw), fh);
	}
	
	// Done
	free(buffer);
	PyFile_DecUseCount((PyFileObject *) ph);
	
	// Return
	output = Py_BuildValue("O", bits);
	return output;
}

PyDoc_STRVAR(readFile_doc, \
"Given a file handle, read in the data and return a list of bits");


/*
  Module Setup - Function Definitions and Documentation
*/

static PyMethodDef DecodeMethods[] = {
	{"readFile", (PyCFunction) readFile, METH_VARARGS, readFile_doc}, 
	{NULL, NULL, 0, NULL}
};

PyDoc_STRVAR(Decode_doc, "Read in OS v2.1 data");


/*
  Module Setup - Initialization
*/

PyMODINIT_FUNC init_decode(void) {
	PyObject *m;

	// Module definitions and functions
	m = Py_InitModule3("_decode", DecodeMethods, Decode_doc);
	
	// Version and revision information
	PyModule_AddObject(m, "__version__", PyString_FromString("0.1"));
	PyModule_AddObject(m, "__revision__", PyString_FromString("$Rev$"));
}