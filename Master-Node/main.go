package main

import (
	"Distributed-Map-Reduce-System/Utils"
	"fmt"
)

func main() {

	masterNode, err := Utils.NewConnectionPool("0.0.0.0:7878")
	masterNode.Start()
	Utils.Panic(err)

	inputs := [][][]string{
		{
			{"aabbb", "ebep", "blablablaa", "hijk", "wsww"},
			{"abba", "eeeppp", "cocor", "ppppppaa", "qwerty"},
			{"lalala", "lalal", "papapa", "aabbb", "acasq"},
		},

		{
			{"a1551a", "parc", "ana", "minim", "1pcl3"},
			{"calabalac", "tivit", "leu", "zece10", "ploaie", "9ana9"},
			{"lalalal", "tema", "papa", "ger"},
		},

		{
			{"apap", "paprc", "apnap", "mipnipm", "copil"},
			{"cepr", "program", "lepu", "zepcep", "golang", "tema"},
			{"par", "impar", "papap", "gepr"},
		},

		{
			{"ana", "parc", "impare", "era", "copil"},
			{"cer", "program", "leu", "alee", "golang", "info"},
			{"inima", "impar", "apa", "eleve"},
		},

		{
			{"acultatef", "parc", "cultateaf", "faculatet", "copil"},
			{"cer", "tatefacul", "leu", "alee", "golang", "ultatefac"},
			{"tefaculta", "impar", "apa", "eleve"},
		},

		{
			{"AcasA", "CasA", "FacultatE", "SisTemE", "distribuite"},
			{"GolanG", "map", "reduce", "Problema", "TemA", "ProieCt"},
			{"LicentA", "semestru", "ALGORitM", "StuDent"},
		},

		{
			{"țânțar", "carte", "ulcior", "copac", "plante"},
			{"beci", "", "mlăștinos", "astronaut", "stele", "planete"},
			{"floare", "somn", "șosetă", "scârțar"},
		},

		{
			{"caracatita", "ceva", "saar", "aaastrfb", ""},
			{"aaabbbccc", "caporal", "ddanube", "jahfjksgfjhs", "ajsdas", "urs"},
			{"scoica", "coral", "arac", "karnak"},
		},

		{
			{"sadsa1@A", "cevaA!4", "saar", "aaastrfb", ""},
			{"aaabbbccc", "!Caporal1", "ddanube", "jahfjksgfjhs", "ajsdas", "urs"},
			{"scoica", "Coral!@12", "arac", "karnak"},
		},

		{
			{"/dev/null", "/bin", "saar", "teme/scoala/2020", ""},
			{"proiect/tema", "/dev", "ddanube", "jahfjksgfjhs", "ajsdas", "urs"},
			{"scoica", "/teme/repos/git", "arac", "karnak"},
		},

		{
			{"Popescu", "Ionescu", "Pop", "aaastrfb", ""},
			{"Nicolae", "Dumitrescu", "ddanube", "jahfjksgfjhs", "ajsdas", "urs"},
			{"Dumitru", "Angelescu", "arac", "karnak"},
		},

		{
			{"12", "5", "6", "13", "7"},
			{"21", "20", "42", "43", "8", "38"},
			{"54", "55", "34", "100"},
		},

		{
			{"1", "13", "6", "7", "9"},
			{"19", "20", "43", "43", "21", "53"},
			{"54", "55", "28", "101"},
		},
	}

	mapTypes := []Utils.MapFunctionId{
		Utils.TypeMap1,
		Utils.TypeMap2,
		Utils.TypeMap3,
		Utils.TypeMap4,
		Utils.TypeMap5,
		Utils.TypeMap6,
		Utils.TypeMap7,
		Utils.TypeMap8,
		Utils.TypeMap9,
		Utils.TypeMap10,
		Utils.TypeMap11,
		Utils.TypeMap12,
		Utils.TypeMap13,
	}

	// Register a new problem for each input and type
	for index, input := range inputs {

		go func() {

			fmt.Println("Input result ", index+1, "is ", masterNode.RegisterProblem(input, mapTypes[index]))

		}()

	}

	// Run the node cluster
	Utils.CreateNodeCluster(4, "127.0.0.1:7878", 2, "../Worker-Nodes/worker-node")

	select {}
}
