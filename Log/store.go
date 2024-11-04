package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian //nos ayudará a guardar los datos en binario
)

const (
	lenWidth = 8 // tamaño de los registros
)

type store struct {
	*os.File               //archivo para guardar datos
	mu       sync.Mutex    //mutex
	buf      *bufio.Writer //escribir en el archivo
	size     uint64        //tamaño en tiempo real del archivo
}

func newStore(f *os.File) (*store, error) {
	//obtenemos la info del archivo en fi
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	//guardamos archivo y generamos nuevo buffer
	//tambien guardamos  tamaño
	return &store{
		File: f,
		size: uint64(fi.Size()),
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append data son los bytes que se guardarán
// pos es posición
// nob (number of bytes) es el numero de bytes y error es si hay un error
func (s *store) Append(data []byte) (nob uint64, pos uint64, err error) {
	//cerramos acceso para evitar modificaciones
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	//vemos en qué posición estamos

	//generará el espacio dentro del buffer con datos aleatorios para ver si cabe el registro
	if errSize := binary.Write(s.buf, enc, uint64(len(data))); errSize != nil {
		return 0, 0, errSize
	}

	//guardamos los datos en el buffer
	writeData, errWrite := s.buf.Write(data)
	if errWrite != nil {
		return 0, 0, errWrite
	}

	// actualizamos el tamaño del buffer
	s.size += uint64(writeData) + lenWidth
	// devolvemos el numero de bytes escritos, la posicion y el error
	return uint64(writeData) + lenWidth, pos, nil

}

func (s *store) Read(pos uint64) ([]byte, error) {
	//cerramos acceso para evitar modificaciones
	s.mu.Lock()
	defer s.mu.Unlock()

	// asegurar que todos los los records estén guardados
	if errRead := s.buf.Flush(); errRead != nil {
		return nil, errRead
	}

	//ver si se puede leer el registro desde la posición
	sizeReg := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(sizeReg, int64(pos)); err != nil {
		return nil, err
	}

	//ahora si slice para leer el registro, aquí se guardará el registro
	recRequest := make([]byte, enc.Uint64(sizeReg))

	//en regBuf se regresarán los datos leidos desde cierta posición y tamaño
	if _, err := s.File.ReadAt(recRequest, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	//regresar los datos leidos
	return recRequest, nil

}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	//se lee buffer completamente
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	//se lee buffer completamente
	errBuf := s.buf.Flush()
	if errBuf != nil {
		return errBuf
	}

	return s.File.Close()

}
