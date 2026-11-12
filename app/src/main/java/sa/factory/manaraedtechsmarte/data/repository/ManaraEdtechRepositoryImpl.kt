package sa.factory.manaraedtechsmarte.data.repository

import sa.factory.manaraedtechsmarte.domain.repository.ManaraEdtechRepository
import sa.factory.manaraedtechsmarte.hardware.HardwareSensorDriver
import sa.factory.manaraedtechsmarte.model.ManaraEdtechItem
import sa.factory.manaraedtechsmarte.model.ManaraEdtechOrder
import sa.factory.manaraedtechsmarte.model.ManaraEdtechTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class ManaraEdtechRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : ManaraEdtechRepository {

    private val _catalog = MutableStateFlow<List<ManaraEdtechItem>>(listOf(
        ManaraEdtechItem("1", "حزمة دروس رياضيات", "منهج تفاعلي مع تصحيح فوري عبر القلم الرقمي وتحليل أداء", 150.0, "معتمد", "رياضيات تفاعلية"),
        ManaraEdtechItem("2", "تجارب علوم افتراضية", "مختبر افتراضي مع محاكاة ثلاثية الأبعاد عبر مستشعرات القلم", 280.0, "فوري", "علوم معززة بالواقع"),
        ManaraEdtechItem("3", "دورات لغات سريعة", "تمارين نطق تفاعلية مع تصحيح عبر BLE Beacon", 95.0, "جديد", "لغات حية")
    ))
    override val catalogFlow: Flow<List<ManaraEdtechItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<ManaraEdtechItem>>(emptyList())
    override val activeCart: StateFlow<List<ManaraEdtechItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<ManaraEdtechTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            ManaraEdtechTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: ManaraEdtechItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: ManaraEdtechItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): ManaraEdtechOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = ManaraEdtechOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
