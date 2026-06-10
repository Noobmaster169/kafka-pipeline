import { Canvas } from "@react-three/fiber";
import { Suspense } from "react";
import { useGLTF, Text, Center, OrbitControls } from "@react-three/drei";
import { useMemo } from "react";
import * as THREE from "three";

const MODEL_BY_TYPE = {
  sedan: "/models/sedan.glb",
  suv: "/models/suv.glb",
  truck: "/models/truck.glb",
  hatchback: "/models/hatchback.glb",
  coupe: "/models/coupe.glb",
};
// Per-model plate offset [x, y, z] — tune once per GLB.
const PLATE_POS = {
  sedan: [0, 0.32, -1.05],
  suv: [0, 0.38, -1.1],
  truck: [0, 0.4, -1.3],
  hatchback: [0, 0.34, -0.95],
  coupe: [0, 0.3, -1.0],
};

// Deterministic hue from the plate text — same plate always gets the same car colour.
function plateHue(plate) {
  return [...(plate || "X")].reduce((n, c) => n + c.charCodeAt(0) * 7, 0) % 360;
}

function CarModel({ type, plate }) {
  const key = MODEL_BY_TYPE[type] ? type : "sedan";
  const { scene } = useGLTF(MODEL_BY_TYPE[key]);

  const cloned = useMemo(() => scene.clone(true), [scene]);

  // Measure the model; pin a plate to each end at bumper height.
  const plates = useMemo(() => {
    const box = new THREE.Box3().setFromObject(cloned);
    const y = box.min.y + (box.max.y - box.min.y) * 0.22;
    return {
      rear:  { position: [0, y, box.min.z - 0.015], rotation: [0, Math.PI, 0] },
      front: { position: [0, y, box.max.z + 0.015], rotation: [0, 0, 0] },
    };
  }, [cloned]);

  const Plate = ({ at }) => (
    <group position={at.position} rotation={at.rotation}>
      <mesh>
        <planeGeometry args={[0.5, 0.13]} />
        <meshBasicMaterial color="#f2f2ea" />
      </mesh>
      <Text fontSize={0.075} color="#15181d" position={[0, 0, 0.002]}>
        {plate}
      </Text>
    </group>
  );

  return (
    <group>
      <primitive object={cloned} />
      <Plate at={plates.rear} />
      <Plate at={plates.front} />
    </group>
  );
}

export default function CarViewer({ type, plate }) {
  const t = (type || "sedan").toLowerCase();
  return (
    <Canvas style={{ height: 230 }} camera={{ position: [2.4, 1.4, -2.6], fov: 35 }} dpr={[1, 2]}>
      <Suspense fallback={null}>
        <ambientLight intensity={0.65} />
        <directionalLight position={[4, 6, 3]} intensity={1.5} />
        <directionalLight position={[-4, 3, -3]} intensity={0.6} />
        <Center>
          <CarModel type={t} plate={plate} />
        </Center>
        <OrbitControls
          enablePan={false}
          enableZoom={false}
          autoRotate
          autoRotateSpeed={1.2}
          minPolarAngle={0.7}
          maxPolarAngle={1.45}
          target={[0, 0.2, 0]}
        />
      </Suspense>
    </Canvas>
  );
}