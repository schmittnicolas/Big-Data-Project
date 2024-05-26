# Dash Application in Python to visualize bourse data

## Prérequis

- 32 Go de stockage disponible
- Assez de RAM
- Se placer dans `/srv/libvirt-workdir` sur les machines de l’école et cloner le projet

## Lancement

- Se placer dans le directory `docker`
1. Démarrer le service de base de données :
   
   ```sh
   docker compose up db
   ```
   Attendre que le service db soit prêt.

2. Démarrer le service d'analyzer:
   
   ```sh
   docker compose up analyzer
   ```
   Ce processus prend environ 50 minutes sur les machines de l'école

3. Une fois l'analyse terminée, démarrer le tableau de bord :

   ```sh
   docker compose up dashboard
   ```

## Analyzer

### Market 
Apres beacoup de recherches sur le site de Boursorma nous avons pu identifier des patterns pour chaque marché: 
 - Paris: Les symboles qui commencent par "1rP" ou "1rEP" 
 - Bruxelles: Les symboles qui commencent par "FF1"
 - Amsterdam: Les symboles qui commencent par "1rA"
 - Nasdaq: Tous les autres symboles qui ne correspondent pas aux préfixes mentionnés
Évidemment, cela est basé sur nos observations personnelles. Nous n'avons rien trouvé de véritablement établi, notamment pour le marché du NASDAQ, dont les données représentent environ 70 % du total et se trouvent dans les fichiers commençant par "amsterdam*".

### Data cleaning: 

Nous avons choisi de supprimer tous les volumes égaux à 0, car cela nous semblait étrange d'avoir des volumes égaux à 0 en plein milieu de la journée.

### Companies 

Nous avons conservé toutes les entreprises, y compris celles qui n'apparaissaient qu'une seule fois au cours des cinq dernières années.

### Insertion

Le temps d'insertion est d'environ 50 minutes. Je tiens à souligner que nous avons développé une version en multi-processing extrêmement performante, mais qu'il est impossible de la faire fonctionner sur les PC de l'EPITA.


## Dashboard

### Selection des actions (tableau en haut à gauche)

- Choix des marchés (plusieurs choix possibles, utiliser la croix pour retirer un choix en scrollant à l'intérieur du dropdown)
- Choix des actions (plusieurs choix possibles, Utiliser la croix pour retirer un choix en scrollant à l'intérieur du dropdown)
- Choix de l'action pour les bandes de Bollinger


### Choix des dates (tableau droite)

- Date de début et date de fin
- Fonctionnalité additionnelle: Date de fin et slider pour choisir l'intervalle (1D, 1W, 1M, 3M, 6M, 1Y, 5Y, 10Y)


### Choix des graphes (tableau gauche)

- 3 graphes sélectionnables avec des onglets


### Tableau récapitulatif à droite

- Affichage des données récapitulatives

