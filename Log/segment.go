package log

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"os"
	"path"
	api "0243179_SistemasDistribuidos/api/v1"

)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

// inicializa un segmento
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	// guardamos el indice del segmento actual que vamos a crear
	curr := s.nextOffset
	record.Offset = curr

	//representamos el record como binario
	data, errRec := proto.Marshal(record)
	if errRec != nil {
		return 0, errRec
	}
	//guardamos los datos del store del record para poder ponerles un indice
	_, pos, errStore := s.store.Append(data)
	if errStore != nil {
		return 0, errStore
	}

	//verificamos si se guarda el store en el index :D
	errInd := s.index.Write(uint32(s.nextOffset-s.baseOffset), pos)
	if errInd != nil {
		return 0, errInd
	}

	//apuntamos al siguiente offset para guardar el siguiente segmento que venga
	s.nextOffset = s.nextOffset + 1
	return curr, nil

}

func (s *segment) Read(offset uint64) (record *api.Record, err error) {
	//leemos dentro del index la posición de nuestro store
	_, pos, errIdx := s.index.Read(int64(offset - s.baseOffset))
	if errIdx != nil {
		return nil, err
	}

	//buscamos el record en store
	data, errStore := s.store.Read(pos)
	if errStore != nil {
		return nil, errStore
	}

	//regresamos nuestro record
	record = &api.Record{}
	errProto := proto.Unmarshal(data, record)
	return record, errProto

}

// IsMaxed vemos si los index y store no sobrepasan su tamaño máximo
func (s *segment) IsMaxed() bool {
	if s.store.size >= s.config.Segment.MaxStoreBytes {
		return true
	}
	if s.index.size >= s.config.Segment.MaxIndexBytes {
		return true
	}
	return false
}

// Remove quitamos los index y store si  no hay errores
func (s *segment) Remove() error {
	errClose := s.Close()
	if errClose != nil {
		return errClose
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// Close cerramos el index y el store
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}
