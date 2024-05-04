/// ce fichier evalue la possibilité de décrire des interfaces pour des agents
/// implémentés, et l'utilisation de fermeture ou configuration de ces objets
/// par iotmonitor, pour permettre l'implémentation d'agents associés
///
/// on aimerai que le modèle propose :
///     une interface typée pour les éléments
///     la capacité à mettre en place des proxys sur des agents pour la construction des flux (composition)
///
/// Voir pour rendre les appels asynchrones (on peut avoir des I/O à réaliser)
///
/// Doit on mettre l'état dans les paramètres d'entrée et de retour ?
///
use std::{error::Error, sync::Arc};

pub struct Event<'a> {
    topic: &'a str,
    payload: &'a str,
}

// interface sur une information typée
pub trait Int<T> {
    fn read(&self, e: Event) -> Result<T, Box<dyn Error>>;
    fn write(&self) -> Result<T, Box<dyn Error>>;
}

// technical interface
type BoolInterface = Box<dyn Int<bool>>;
type F32Interface = Box<dyn Int<f32>>;

// interface meaning
enum SemanticInterface {
    // fonction,
    OnOff(BoolInterface),
    Temperature(F32Interface),
    Humidity(F32Interface),
    BatterieEnergie(F32Interface),
    PowerElectric(F32Interface),
    EtatConnection(BoolInterface),
}

struct ClosureImple<T, C, V>
where
    C: Fn(Event) -> Result<T, Box<dyn Error>>,
    V: Fn() -> Result<T, Box<dyn Error>>,
{
    pub read: C,
    pub write: V,
}

impl<T, C: Fn(Event) -> Result<T, Box<dyn Error>>, V: Fn() -> Result<T, Box<dyn Error>>> Int<T>
    for ClosureImple<T, C, V>
{
    fn read(&self, e: Event) -> Result<T, Box<dyn Error>> {
        (self.read)(e)
    }
    fn write(&self) -> Result<T, Box<dyn Error>> {
        (self.write)()
    }
}

fn interface<T, C: Fn(Event) -> Result<T, Box<dyn Error>>, V: Fn() -> Result<T, Box<dyn Error>>>(
    r: C,
    v: V,
) -> Box<ClosureImple<T, C, V>> {
    Box::new(ClosureImple { read: r, write: v })
}

///////////////////////////////////////////////////////////////////////////////////////////////
// the management or iot management using scripting , is hardly
// tight to iot definition and explaination, using a specific language that is the most
// appropriate to the constructed system

enum TypesAppareils {
    Lumiere,
    Electromenager,
    Jardin,
}

enum Discriminants {
    NomService(String), // eclairage
    Localisation(String),
    TypeAppareil(TypesAppareils),
}

struct NamedSemanticInterface<'a> {
    name: &'a str,
    interface: SemanticInterface,
}

trait Obj<'a> {
    fn name(&self) -> &'a str; // name associated to a bunch of sensors, or logics
    fn tags(&self) -> Vec<Discriminants>; // discriminents
    fn interfaces(&self) -> Vec<SemanticInterface>;
    fn injected(&self) -> Vec<RefSemanticInterface<'a>>;
}

struct RefSemanticInterface<'a> {
    needs: Vec<NamedSemanticInterface<'a>>,
}

//////////////////////////////////////////////////////////////////////////////////////////
// application test

struct BaseObj<'a> {
    name: &'a str,            // name associated to a bunch of sensors, or logics
    tags: Vec<Discriminants>, // discriminents
    interfaces: Vec<SemanticInterface>,
    injected: Vec<RefSemanticInterface<'a>>,
}



//////////////////////////////////////////////////////////////////////////////////////////////


// il faut pouvoir marquer les changements d'état, idempotence

// // macro discriminant, classement
// struct EclairageBureau {


//     // définition de la plateforme

//     // semanticinterface []
//     // adapter (transformation évènement MQTT -> valeur, en lecture, ou en écriture)
//     ledstrip: F32Interface,

//     // semanticinterface
//     // adapter
//     presence: BoolInterface,


//     // injected
//     command_bureau: RefSemanticInterface(bool),


        // stateValue: serializable

//     fn onChangeLedStrip(newValue : T ) -> {


//         presence.write(T2); // <- marqueur de pilotage, écriture

//     }

//     // sur des changement de timers
//     fn onTimerChange(... ) {

//     }

// }


//////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_description() {
    let ctx = Arc::new(3);

    // implementation on a bool information
    let ibool = interface(
        move |e| {
            let c = *ctx + 1;
            Ok(true)
        },
        || Ok(true),
    );

    // objects are defined with a name, categories
    let o: Obj = Obj {
        name: "monobjet",
        tags: vec![],
        injected: vec![],
        interfaces: vec![SemanticInterface::OnOff(ibool)],
    };

    // use the interface, call functions
    let v = &o.interfaces[0];
    match v {
        SemanticInterface::OnOff(b) => {
            let event = Event {
                payload: "",
                topic: "",
            };
            let b = b.read(event).unwrap();
        }
        SemanticInterface::Temperature(t) => {}
        _ => (),
    }
}
