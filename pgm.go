package main
import (
	"fmt"
	"sync"
	"github.com/satori/go.uuid"
)

type PGM struct{
	Vertices map[uuid.UUID]*Vertex
	Edges map[uuid.UUID]*Edge
	storageMutex  sync.Mutex
	TotalEntityCount float64
	TotalVertexCount float64
	TotalEdgeCount   float64
	UniqueVertexNameMap map[string]uuid.UUID
	UniqueEdgeNameMap map[string]uuid.UUID
}

type Entity struct {
	Id uuid.UUID
	Name string
	UniqueName string
	Label string
	Count float64
	Properties map[string]interface{}


}
type Vertex struct {
	Entity
	Edges map[uuid.UUID]*Edge
}

type Edge struct {
	Entity
	VertexA *Vertex
	VertexB *Vertex
	Directionality string
}


func (p *PGM) GetEdgeById(id uuid.UUID) (*Edge, bool) {
	e,ok := p.Edges[id]
	return e,ok
}

func (p *PGM) GetVertexById(id uuid.UUID) (*Vertex, bool){
	v,ok := p.Vertices[id]
	return v,ok
}

func (e *Entity) Increment(){
	e.Count += 1.0

}
func (e *Entity) UpdateProperties(props, overwriteProps map[string]interface{}){
	//favor already existing properties
	for k, v := range props{
		if _,ok := e.Properties[k]; ok == false{
			e.Properties[k] = v
		}
	}

	//overwrite any explcitly overwritten properties
	for k,v := range overwriteProps{
		if _,ok := e.Properties[k]; ok{
			e.Properties[k] = v
		}
	}
	e.Properties = props
	e.Increment()
}

func (p *PGM) UpsertVertex(vb *Vertex) *Vertex{
	p.storageMutex.Lock()
	defer p.storageMutex.Unlock()
	var _vid uuid.UUID
	if vid,ok := p.UniqueVertexNameMap[vb.UniqueName]; ok{
		_vid = vid
		p.Vertices[vid].UpdateProperties(vb.Properties, map[string]interface{}{})
	}else{
		//new
		_vid = uuid.NewV4()
		vb.Id = _vid
		vb.Count = 1.0
		vb.Edges = make(map[uuid.UUID]*Edge)
		p.Vertices[vb.Id] = vb
		//unique name to id
		p.UniqueVertexNameMap[vb.UniqueName] = _vid
		p.TotalVertexCount += 1.0
		p.TotalEntityCount += 1.0
		
	}
	return p.Vertices[_vid]
}


func (p *PGM) UpsertEdge(ea *Edge) *Edge{
	p.storageMutex.Lock()
	defer p.storageMutex.Unlock()
	var _eid uuid.UUID

	if eid,ok := p.UniqueEdgeNameMap[ea.UniqueName]; ok{
		_eid = eid
		//update properties
		p.Edges[eid].UpdateProperties(ea.Properties, map[string]interface{}{})
	}else{
		//new
		_eid = uuid.NewV4()
		ea.Id = _eid
		p.Edges[ea.Id] = ea
		ea.Count = 1.0
		//unique name to id
		p.UniqueEdgeNameMap[ea.UniqueName] = _eid
		p.TotalEdgeCount += 1.0
		p.TotalEntityCount += 1.0
	}
	return p.Edges[_eid] 
}

func (v *Vertex) UpsertEdge(e *Edge) *Edge {
	var _edge *Edge
	if edge, ok := v.Edges[e.Id]; ok == false{
		v.Edges[e.Id] = e
		_edge = edge
	}else{
		_edge = edge
	}
	return _edge
}


func NewPgm() *PGM{
	pgm := &PGM{}
	pgm.Vertices = make(map[uuid.UUID]*Vertex)
	pgm.Edges = make(map[uuid.UUID]*Edge)

	pgm.UniqueVertexNameMap = make(map[string]uuid.UUID)
	pgm.UniqueEdgeNameMap = make(map[string]uuid.UUID)
	return pgm
}


func main(){
	test := [][]string{
		[]string{"what","is","thought","eric","baum"},
		[]string{"what","is","happiness","eric","whitegate"},
		[]string{"what","do","happiness","and","fear", "have", "in", "common"},
		[]string{"on","the","nature","of","things","whitegate"},
		[]string{"what","is","the","meaning","of","this"},
	}



//insert data into graph
	pgm := NewPgm()

	for _,sentence := range test {
		for word_idx := 0; word_idx <len(sentence)-1; word_idx+=2{

			va := &Vertex{Entity{UniqueName: sentence[word_idx]},nil}
			va = pgm.UpsertVertex(va)

			vb := &Vertex{Entity{UniqueName: sentence[word_idx+1]},nil}
			vb = pgm.UpsertVertex(vb)

			e := &Edge{Entity{UniqueName: sentence[word_idx]+"_"+sentence[word_idx+1]},va,vb,"yo"}
			e = pgm.UpsertEdge(e)
			
			va.UpsertEdge(e)
			vb.UpsertEdge(e)
		}
	}





//check that probabilities are correct
	what_v := pgm.Vertices[pgm.UniqueVertexNameMap["what"]]
	fmt.Printf("probability of global %s: %v\n", what_v.UniqueName, what_v.Count/pgm.TotalVertexCount)
	for _,e := range what_v.Edges {
		fmt.Printf("probability of global %s: %v\n", e.UniqueName, e.Count/pgm.TotalEdgeCount)
		edge_sum := 0.0
		for _,_e := range what_v.Edges{
			edge_sum += _e.Count
		}
		fmt.Printf("probability of %s coming after %s: %v\n", e.VertexB.UniqueName,e.VertexA.UniqueName, e.Count/edge_sum)
	}
}