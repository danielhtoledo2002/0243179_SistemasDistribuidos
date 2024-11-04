package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	//sacamos info del archivo
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	//guardamos el tamaño del archivo
	sizeFile := uint64(fi.Size())

	//ajusta tamaño del archivo
	if errTrunc := os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); errTrunc != nil {
		return nil, errTrunc
	}

	//mapeamos en la memoria el archivo
	newmmap, errMap := gommap.Map(f.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if errMap != nil {
		return nil, errMap
	}

	return &index{
		file: f,
		mmap: newmmap,
		size: sizeFile,
	}, nil
}

func (i *index) Name() string {
	return i.file.Name()
}

// tomamos un index (idx)
func (i *index) Read(idx int64) (out uint32, pos uint64, err error) {
	//ver si el index no está vacio
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if idx == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(idx)
	}

	if (uint64(out)*entWidth)+entWidth > i.size {
		return 0, 0, io.EOF
	} else {
		pos = uint64(out) * entWidth
	}
	//se leerá el offset, decodificado
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	//se leerá la posición, decodificado
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])

	return out, pos, nil

}

func (i *index) Write(off uint32, pos uint64) error {

	//verifiquemos si el nuevo dato cabe en memoria
	memo := uint64(len(i.mmap))
	if memo < i.size+entWidth {
		return io.EOF
	}

	//escribimos el offset
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	//escribimos la posición después del offset
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	//actualizamos el tamaño del index
	i.size += entWidth
	return nil
}

func (i *index) Close() error {

	//cerramos el archivo :D
	return i.file.Close()

}
