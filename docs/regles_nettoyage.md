# Journal d'observations & Règles de nettoyage — Dataset e‑commerce

Ce document regroupe les observations de qualité de données et les règles de nettoyage associées, prêtes à être traduites en tests/transformations de pipeline.

## 1. Observations qualité (synthèse)

### 1.1 Colonnes avec valeurs manquantes (dates clés)

**Constat :** des valeurs manquantes existent sur les dates de livraison/expédition ; les autres colonnes critiques sont complètes.

- **order_delivered_customer_date** — beaucoup de valeurs manquantes (commandes non livrées ou information manquante).
- **order_delivered_carrier_date** — beaucoup de valeurs manquantes (commandes non expédiées ou information manquante).
- **order_approved_at** — quelques valeurs manquantes inattendues : bloque la mesure du délai achat → approbation.
- **Colonnes critiques (IDs)** — aucune valeur manquante (order_id, customer_id, product_id, seller_id…).

### 1.2 Cardinalité des colonnes

**Constat :** forte cardinalité logique sur IDs et dates ; faible/moyenne sur statuts et date estimée.

- Identifiants (order_id, customer_id) — très forte cardinalité : attendu.
- Dates (purchase, approved, delivered_*) — forte cardinalité : attendu.
- **order_estimated_delivery_date** — cardinalité moyenne.
- **order_status** — faible cardinalité.

### 1.3 Doublons sur clés primaires (PK)

**Constat :** aucune table ne présente de doublons de PK, sauf `order_reviews` (doublons sur review_id).

### 1.4 Doublons hors PK

**Constat :** aucune table métier n’a de doublons hors PK, sauf la table **geolocation** qui comporte de nombreuses répétitions.

### 1.5 Cohérence des jointures (FK)

- orders.customer_id → customers.customer_id — aucune ligne orpheline.
- order_items.order_id → orders.order_id — aucune ligne orpheline.
- order_items.product_id → products.product_id — aucune ligne orpheline.

## 2. Règles de nettoyage (WHEN / THEN)

### 2.1 Dates de livraison et d’expédition manquantes

**Règle :** ces NULL traduisent un état métier (non livré / non expédié). **On ne remplit pas artificiellement.**

- WHEN statut ≠ livré/expédié AND date est NULL → THEN conserver NULL.
- WHEN order_status = delivered AND order_delivered_customer_date IS NULL → THEN `qc_missing_delivered_customer_date = TRUE`.
- WHEN statut ≥ expédié AND order_delivered_carrier_date IS NULL → THEN `qc_missing_carrier_date = TRUE`.

### 2.2 Date d’approbation manquante (`order_approved_at`)

**Règle :** pas d’imputation par défaut.

- WHEN order_approved_at IS NULL → THEN `approved_at_missing = TRUE`.
- WHEN validation métier = auto-approbation → THEN `order_approved_at = order_purchase_timestamp` et `approved_at_source = imputed_from_purchase`.

### 2.3 Ordre logique des dates (chronologie)

Les dates doivent suivre : purchase → approved → delivered_carrier → delivered_customer → estimated_delivery.

- WHEN incohérence (ex. delivered_customer < purchase) → THEN `qc_temporal_inconsistency = TRUE` + exclusion des KPI.

### 2.4 Identifiants et typage

- Unicité : order_id, customer_id, product_id, seller_id.
- Typage homogène : DATE, STRING, NUMÉRIQUE, FLOAT/DECIMAL.

### 2.5 Dédoublonnage des avis (`order_reviews`)

- WHEN doublons sur review_id → THEN conserver **1 version canonique**.
- Créer `order_reviews_canonique` + `is_canonical = FALSE` sur les autres lignes.

### 2.6 Déduplication de `geolocation`

Créer vue `geolocation_dedup` (granularité : code postal + ville + lat/long arrondies).

### 2.7 Intégrité des clés étrangères (FK)

Contrôle à chaque run :

- orders.customer_id ∈ customers.customer_id
- order_items.order_id ∈ orders.order_id
- order_items.product_id ∈ products.product_id
- order_items.seller_id ∈ sellers.seller_id
- order_reviews.order_id ∈ orders.order_id

### 2.8 Traçabilité et gouvernance

- Ajouter colonnes de traçabilité (`*_missing`, `*_imputed`, `qc_*`).
- Jobs idempotents → UPSERT/MERGE.
- Standardiser noms et types.
